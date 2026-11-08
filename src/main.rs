use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::fs;

// Declares src/notebook.rs as a module of this crate.
mod notebook;
mod spark;

// Base path for all Unity Catalog endpoints
const UC: &str = "/api/2.1/unity-catalog";

// Catalogs we don't walk (system has hundreds of tables; internal is Databricks-managed)
const SKIP_CATALOGS: [&str; 2] = ["system", "__databricks_internal"];

/// Builds a full URL and safely URL-encodes the query parameters
/// (so a name like "my catalog" or a page token with '=' doesn't break the URL).
fn build_url(host: &str, path: &str, params: &[(&str, &str)]) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&format!("{}{}", host.trim_end_matches('/'), path))
        .with_context(|| format!("bad URL for path {path}"))?;
    if !params.is_empty() {
        url.query_pairs_mut().extend_pairs(params);
    }
    Ok(url)
}

/// Everything a request needs, plus a list of errors we collect instead of crashing on.
struct Ctx {
    client: reqwest::Client,
    host: String,
    token: String,
    dir: String,
    errors: Vec<Value>,
}

impl Ctx {
    /// One authenticated GET returning JSON. Errors are returned to the caller.
    async fn request(&self, path: &str, params: &[(&str, &str)]) -> Result<Value> {
        let url = build_url(&self.host, path, params)?;
        let resp = self
            .client
            .get(url.clone())
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| format!("request to {url} failed"))?
            .error_for_status()
            .with_context(|| format!("{url} returned an error status"))?;
        Ok(resp.json().await?)
    }

    /// Like `request`, but a failure is printed and recorded, and we get `None`.
    /// One blocked endpoint (403/404) shouldn't stop the whole run.
    async fn get(&mut self, label: &str, path: &str, params: &[(&str, &str)]) -> Option<Value> {
        match self.request(path, params).await {
            Ok(v) => Some(v),
            Err(e) => {
                println!("FAIL {label}: {e:#}");
                self.errors.push(json!({ "label": label, "error": format!("{e:#}") }));
                None
            }
        }
    }

    /// Follows `next_page_token` until the list is complete.
    /// `key` is the JSON field holding the items (e.g. "tables", "catalogs").
    async fn get_all(
        &mut self,
        label: &str,
        path: &str,
        params: &[(&str, &str)],
        key: &str,
    ) -> Vec<Value> {
        let mut items = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut p: Vec<(&str, &str)> = params.to_vec();
            if let Some(t) = &page_token {
                p.push(("page_token", t.as_str()));
            }
            let Some(page) = self.get(label, path, &p).await else { break };
            if let Some(arr) = page[key].as_array() {
                items.extend(arr.iter().cloned());
            }
            match page["next_page_token"].as_str() {
                Some(t) if !t.is_empty() => page_token = Some(t.to_string()),
                _ => break,
            }
        }
        items
    }

    fn save(&self, name: &str, value: &Value) -> Result<()> {
        let file = format!("{}/{}.json", self.dir, name);
        fs::write(&file, serde_json::to_string_pretty(value)?)
            .with_context(|| format!("could not write {file}"))?;
        println!("ok   {name}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    // `cargo run -- notebook ...` goes to the notebook tool; anything else
    // runs the full snapshot below, exactly as before.
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("notebook") {
        return notebook::run(&args[1..]).await;
    }

    if args.first().map(String::as_str) == Some("spark") {
        return spark::run(&args[1..]).await;
    }

    let host = std::env::var("DATABRICKS_HOST").context("DATABRICKS_HOST not set")?;
    let token = std::env::var("DATABRICKS_TOKEN").context("DATABRICKS_TOKEN not set")?;

    // Each run gets its own folder named after the Unix time in seconds
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let dir = format!("snapshots/{ts}");
    fs::create_dir_all(&dir)?;

    let mut ctx = Ctx {
        client: reqwest::Client::new(),
        host,
        token,
        dir,
        errors: Vec::new(),
    };
    println!("Collecting into {}\n", ctx.dir);

    // ---- 1. Simple endpoints: one request, one file -------------------------
    let simple = [
        ("me", "/api/2.0/preview/scim/v2/Me"),
        ("users", "/api/2.0/preview/scim/v2/Users"),
        ("groups", "/api/2.0/preview/scim/v2/Groups"),
        ("service_principals", "/api/2.0/preview/scim/v2/ServicePrincipals"),
        ("clusters", "/api/2.0/clusters/list"),
        ("warehouses", "/api/2.0/sql/warehouses"),
        ("instance_pools", "/api/2.0/instance-pools/list"),
        ("cluster_policies", "/api/2.0/policies/clusters/list"),
        ("repos", "/api/2.0/repos"),
        ("pipelines", "/api/2.0/pipelines"),
        ("serving_endpoints", "/api/2.0/serving-endpoints"),
        ("secret_scopes", "/api/2.0/secrets/scopes/list"),
        ("pat_tokens", "/api/2.0/token/list"), // metadata only, never the secret value
        ("metastore_summary", "/api/2.1/unity-catalog/metastore_summary"),
        ("current_metastore", "/api/2.1/unity-catalog/current-metastore-assignment"),
    ];
    for (name, path) in simple {
        if let Some(v) = ctx.get(name, path, &[]).await {
            ctx.save(name, &v)?;
        }
    }

    // ---- 2. Paginated lists --------------------------------------------------
    let jobs = ctx
        .get_all("jobs", "/api/2.1/jobs/list", &[("expand_tasks", "true")], "jobs")
        .await;
    ctx.save("jobs", &json!(jobs))?;

    let runs = ctx.get_all("job_runs", "/api/2.1/jobs/runs/list", &[], "runs").await;
    ctx.save("job_runs", &json!(runs))?;

    // ---- 3. Workspace tree (notebooks, files, folders) ----------------------
    // Walk folders with a stack instead of recursion (async recursion is awkward in Rust).
    let mut tree: Vec<Value> = Vec::new();
    let mut stack = vec!["/".to_string()];
    while let Some(path) = stack.pop() {
        let Some(v) = ctx
            .get("workspace_list", "/api/2.0/workspace/list", &[("path", path.as_str())])
            .await
        else {
            continue;
        };
        for obj in v["objects"].as_array().into_iter().flatten() {
            if obj["object_type"] == "DIRECTORY" || obj["object_type"] == "REPO" {
                if let Some(p) = obj["path"].as_str() {
                    stack.push(p.to_string());
                }
            }
            tree.push(obj.clone());
        }
    }
    ctx.save("workspace_tree", &json!(tree))?;

    // ---- 4. Unity Catalog: account-level objects ----------------------------
    let metastores = ctx
        .get_all("metastores", &format!("{UC}/metastores"), &[], "metastores")
        .await;
    ctx.save("metastores", &json!(metastores))?;

    let ext_locations = ctx
        .get_all(
            "external_locations",
            &format!("{UC}/external-locations"),
            &[],
            "external_locations",
        )
        .await;
    ctx.save("external_locations", &json!(ext_locations))?;

    let credentials = ctx
        .get_all(
            "storage_credentials",
            &format!("{UC}/storage-credentials"),
            &[],
            "storage_credentials",
        )
        .await;
    ctx.save("storage_credentials", &json!(credentials))?;

    let connections = ctx
        .get_all("connections", &format!("{UC}/connections"), &[], "connections")
        .await;
    ctx.save("connections", &json!(connections))?;

    // ---- 5. Unity Catalog: catalogs -> schemas -> tables / volumes ----------
    let catalogs = ctx.get_all("catalogs", &format!("{UC}/catalogs"), &[], "catalogs").await;
    ctx.save("catalogs", &json!(catalogs))?;

    let mut schemas: Vec<Value> = Vec::new();
    let mut tables: Vec<Value> = Vec::new(); // each table already includes its columns
    let mut volumes: Vec<Value> = Vec::new();
    let mut walked_catalogs: Vec<String> = Vec::new();

    for cat in &catalogs {
        let Some(cat_name) = cat["name"].as_str() else { continue };
        if SKIP_CATALOGS.contains(&cat_name) {
            println!("skip catalog {cat_name}");
            continue;
        }
        walked_catalogs.push(cat_name.to_string());

        let cat_schemas = ctx
            .get_all(
                &format!("schemas:{cat_name}"),
                &format!("{UC}/schemas"),
                &[("catalog_name", cat_name)],
                "schemas",
            )
            .await;

        for sch in &cat_schemas {
            let Some(sch_name) = sch["name"].as_str() else { continue };
            if sch_name == "information_schema" {
                continue;
            }
            let params = [("catalog_name", cat_name), ("schema_name", sch_name)];
            let label = format!("{cat_name}.{sch_name}");

            tables.extend(
                ctx.get_all(&format!("tables:{label}"), &format!("{UC}/tables"), &params, "tables")
                    .await,
            );
            volumes.extend(
                ctx.get_all(&format!("volumes:{label}"), &format!("{UC}/volumes"), &params, "volumes")
                    .await,
            );
        }
        schemas.extend(cat_schemas);
    }
    ctx.save("schemas", &json!(schemas))?;
    ctx.save("tables", &json!(tables))?;
    ctx.save("volumes", &json!(volumes))?;

    // ---- 6. Privileges (grants) on every securable we found -----------------
    // (securable type used in the URL, full name)
    let mut targets: Vec<(&str, String)> = Vec::new();
    for c in &walked_catalogs {
        targets.push(("catalog", c.clone()));
    }
    for s in &schemas {
        if let Some(n) = s["full_name"].as_str() {
            targets.push(("schema", n.to_string()));
        }
    }
    for t in &tables {
        if let Some(n) = t["full_name"].as_str() {
            targets.push(("table", n.to_string()));
        }
    }
    for e in &ext_locations {
        if let Some(n) = e["name"].as_str() {
            targets.push(("external_location", n.to_string()));
        }
    }
    for c in &credentials {
        if let Some(n) = c["name"].as_str() {
            targets.push(("storage_credential", n.to_string()));
        }
    }

    let mut grants = serde_json::Map::new();
    for (kind, full_name) in targets {
        let label = format!("grants:{kind}:{full_name}");
        let path = format!("{UC}/permissions/{kind}/{full_name}");
        if let Some(v) = ctx.get(&label, &path, &[]).await {
            grants.insert(format!("{kind}:{full_name}"), v);
        }
    }
    ctx.save("grants", &Value::Object(grants))?;

    // ---- 7. Errors + summary --------------------------------------------------
    ctx.save("errors", &json!(ctx.errors))?;

    println!("\n==== Summary ====");
    println!("catalogs:            {}", catalogs.len());
    println!("schemas:             {}", schemas.len());
    println!("tables:              {}", tables.len());
    println!("volumes:             {}", volumes.len());
    println!("external locations:  {}", ext_locations.len());
    println!("storage credentials: {}", credentials.len());
    println!("workspace objects:   {}", tree.len());
    println!("jobs:                {}", jobs.len());
    println!("errors:              {}", ctx.errors.len());
    println!("Saved in {}", ctx.dir);
    Ok(())
}
