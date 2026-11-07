// Notebook helpers: list notebooks, split one into cells, run a single cell.
//
// Databricks has no "run cell N" endpoint. So we:
//   1. export the notebook source text (workspace/export),
//   2. split it into cells ourselves,
//   3. send that cell's code to a running cluster (Command Execution API, /api/1.2).

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::time::Duration;

/// A tiny authenticated HTTP client for this file.
struct Api {
    http: reqwest::Client,
    host: String,
    token: String,
}

impl Api {
    fn from_env() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::new(),
            host: std::env::var("DATABRICKS_HOST").context("DATABRICKS_HOST not set")?,
            token: std::env::var("DATABRICKS_TOKEN").context("DATABRICKS_TOKEN not set")?,
        })
    }

    /// Reads the body as text first so a Databricks error message
    /// (which explains *why* it failed) is not thrown away.
    async fn finish(resp: reqwest::Response) -> Result<Value> {
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("HTTP {status}: {text}");
        }
        serde_json::from_str(&text).context("response was not valid JSON")
    }

    async fn get(&self, path: &str, params: &[(&str, &str)]) -> Result<Value> {
        // Url::query_pairs_mut percent-encodes values like "/Users/me@x.com/My Notebook"
        let mut url = reqwest::Url::parse(&format!("{}{}", self.host.trim_end_matches('/'), path))?;
        url.query_pairs_mut().extend_pairs(params);
        let resp = self.http.get(url).bearer_auth(&self.token).send().await?;
        Self::finish(resp).await
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value> {
        let url = format!("{}{}", self.host.trim_end_matches('/'), path);
        let resp = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?;
        Self::finish(resp).await
    }
}

/// Entry point called from main: `cargo run -- notebook <subcommand> ...`
pub async fn run(args: &[String]) -> Result<()> {
    let api = Api::from_env()?;
    match args.first().map(String::as_str) {
        Some("list") => list_notebooks(&api).await,
        Some("cells") => {
            let path = args.get(1).context("usage: notebook cells <path>")?;
            show_cells(&api, path).await
        }
        Some("run-cell") => {
            let (Some(path), Some(idx), Some(cluster)) = (args.get(1), args.get(2), args.get(3))
            else {
                bail!("usage: notebook run-cell <path> <cell_index> <cluster_id>");
            };
            let idx: usize = idx.parse().context("cell_index must be a number")?;
            run_cell(&api, path, idx, cluster).await
        }
        _ => bail!("usage: notebook <list | cells <path> | run-cell <path> <index> <cluster_id>>"),
    }
}

/// Walk the whole workspace and print every NOTEBOOK.
async fn list_notebooks(api: &Api) -> Result<()> {
    // A stack of folders still to visit (simpler than async recursion).
    let mut stack = vec!["/".to_string()];
    let mut count = 0;
    while let Some(dir) = stack.pop() {
        let resp = match api.get("/api/2.0/workspace/list", &[("path", &dir)]).await {
            Ok(v) => v,
            Err(e) => {
                println!("skip {dir}: {e:#}"); // e.g. no permission; keep going
                continue;
            }
        };
        for obj in resp["objects"].as_array().into_iter().flatten() {
            let path = obj["path"].as_str().unwrap_or("?");
            match obj["object_type"].as_str() {
                Some("DIRECTORY") | Some("REPO") => stack.push(path.to_string()),
                Some("NOTEBOOK") => {
                    println!("{:<8} {path}", obj["language"].as_str().unwrap_or("?"));
                    count += 1;
                }
                _ => {}
            }
        }
    }
    println!("\n{count} notebooks");
    Ok(())
}

/// Download a notebook as source text and return (language, cells).
async fn load_cells(api: &Api, path: &str) -> Result<(String, Vec<String>)> {
    let status = api
        .get("/api/2.0/workspace/get-status", &[("path", path)])
        .await?;
    let language = status["language"]
        .as_str()
        .context("not a notebook (no language field)")?
        .to_string();

    let exported = api
        .get(
            "/api/2.0/workspace/export",
            &[("path", path), ("format", "SOURCE")],
        )
        .await?;
    // The file content comes back base64-encoded inside JSON.
    let bytes = STANDARD.decode(exported["content"].as_str().context("no content")?)?;
    let source = String::from_utf8(bytes).context("notebook is not valid UTF-8")?;

    Ok((language.clone(), split_cells(&source, &language)))
}

/// Comment prefix used by the notebook's language in exported source files.
fn comment_prefix(language: &str) -> &'static str {
    match language {
        "SQL" => "-- ",
        "SCALA" => "// ",
        _ => "# ", // PYTHON and R
    }
}

/// Exported source looks like:
///   # Databricks notebook source
///   <cell 0>
///   # COMMAND ----------
///   <cell 1>
/// so we split on the COMMAND marker line.
fn split_cells(source: &str, language: &str) -> Vec<String> {
    let prefix = comment_prefix(language);
    let header = format!("{prefix}Databricks notebook source");
    let marker = format!("{prefix}COMMAND ----------");

    source
        .strip_prefix(&header)
        .unwrap_or(source)
        .split(&marker)
        .map(|c| c.trim().to_string())
        .collect()
}

async fn show_cells(api: &Api, path: &str) -> Result<()> {
    let (language, cells) = load_cells(api, path).await?;
    println!("{path} ({language}), {} cells\n", cells.len());
    for (i, cell) in cells.iter().enumerate() {
        let first = cell.lines().next().unwrap_or("");
        println!("[{i}] {first}");
    }
    Ok(())
}

/// Turn a raw cell into (language, code).
/// Cells like `%sql` are stored as `# MAGIC %sql` / `# MAGIC select 1`.
fn prepare_cell(cell: &str, notebook_language: &str) -> Result<(String, String)> {
    let magic = format!("{}MAGIC", comment_prefix(notebook_language));
    let is_magic = !cell.is_empty() && cell.lines().all(|l| l.starts_with(&magic));
    if !is_magic {
        return Ok((notebook_language.to_lowercase(), cell.to_string()));
    }

    // Remove "# MAGIC" and the single space after it from every line.
    let stripped: Vec<&str> = cell
        .lines()
        .map(|l| {
            let rest = l.strip_prefix(&magic).unwrap_or(l);
            rest.strip_prefix(' ').unwrap_or(rest)
        })
        .collect();

    let first = stripped.first().copied().unwrap_or("").trim();
    let lang = match first {
        "%sql" => "sql",
        "%python" => "python",
        "%scala" => "scala",
        "%r" => "r",
        other => bail!("cell magic `{other}` can't be run through the command API"),
    };
    Ok((lang.to_string(), stripped[1..].join("\n")))
}

async fn run_cell(api: &Api, path: &str, index: usize, cluster_id: &str) -> Result<()> {
    let (nb_lang, cells) = load_cells(api, path).await?;
    let cell = cells
        .get(index)
        .with_context(|| format!("cell {index} doesn't exist; notebook has {}", cells.len()))?;
    let (lang, code) = prepare_cell(cell, &nb_lang)?;

    println!("--- running cell {index} as {lang} ---\n{code}\n---------------------------");

    // The cluster must already be running; we won't start it for you.
    let cluster = api
        .get("/api/2.0/clusters/get", &[("cluster_id", cluster_id)])
        .await?;
    let state = cluster["state"].as_str().unwrap_or("UNKNOWN");
    if state != "RUNNING" {
        bail!("cluster {cluster_id} is {state}, not RUNNING. Start it first.");
    }

    // A "context" is an execution session (like a REPL) on the cluster.
    let ctx = api
        .post(
            "/api/1.2/contexts/create",
            &json!({ "language": lang, "clusterId": cluster_id }),
        )
        .await?;
    let context_id = ctx["id"].as_str().context("no context id")?.to_string();

    // Run, but always clean up the context afterwards, even on error.
    let result = execute(api, cluster_id, &context_id, &lang, &code).await;
    let _ = api
        .post(
            "/api/1.2/contexts/destroy",
            &json!({ "clusterId": cluster_id, "contextId": context_id }),
        )
        .await;
    result
}

/// Submit the command, then poll its status until it finishes.
async fn execute(api: &Api, cluster_id: &str, context_id: &str, lang: &str, code: &str) -> Result<()> {
    let cmd = api
        .post(
            "/api/1.2/commands/execute",
            &json!({
                "language": lang,
                "clusterId": cluster_id,
                "contextId": context_id,
                "command": code,
            }),
        )
        .await?;
    let command_id = cmd["id"].as_str().context("no command id")?.to_string();

    loop {
        let st = api
            .get(
                "/api/1.2/commands/status",
                &[
                    ("clusterId", cluster_id),
                    ("contextId", context_id),
                    ("commandId", &command_id),
                ],
            )
            .await?;
        match st["status"].as_str() {
            Some("Finished") | Some("Error") | Some("Cancelled") => {
                print_results(&st);
                return Ok(());
            }
            other => {
                println!("status: {}", other.unwrap_or("?"));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

fn print_results(status: &Value) {
    let r = &status["results"];
    match r["resultType"].as_str() {
        Some("text") => println!("{}", r["data"].as_str().unwrap_or("")),
        Some("table") => {
            println!("columns: {}", r["schema"]);
            println!("rows:    {}", r["data"]);
        }
        Some("error") => println!(
            "ERROR: {}\n{}",
            r["summary"].as_str().unwrap_or(""),
            r["cause"].as_str().unwrap_or("")
        ),
        _ => println!("{}", serde_json::to_string_pretty(status).unwrap_or_default()),
    }
}

// Pure logic, no network: easy to unit test.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_python_cells() {
        let src = "# Databricks notebook source\nprint(1)\n\n# COMMAND ----------\n\nprint(2)\n";
        assert_eq!(split_cells(src, "PYTHON"), vec!["print(1)", "print(2)"]);
    }

    #[test]
    fn strips_sql_magic() {
        let (lang, code) = prepare_cell("# MAGIC %sql\n# MAGIC select 1", "PYTHON").unwrap();
        assert_eq!((lang.as_str(), code.as_str()), ("sql", "select 1"));
    }
}
