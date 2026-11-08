// Spark configuration tool.
// cargo run -- spark show [filer]
//
// Configs are read by running SQL (`SET -v`) on a SQL warehouse through the
// Statement Execution API: POST the statement, then poll until it finishes.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::time::Duration;

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

    /// Read the body as text first so Databrick's error message isn't lost.
    async fn finish(resp: reqwest::Response) -> Result<Value> {
        let status = resp.status();
        let text =  resp.text().await?;
        if !status.is_success() {
            bail!("HTTP {status}: {text}");
        }
    }
}

