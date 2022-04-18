use async_trait::async_trait;

use datafusion::datasource::clickhouse::MergeTree;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("ontime", Arc::new(MergeTree::new()))?;

    let df = ctx.sql("SELECT * FROM ontime;").await?;
    df.show().await?;
    Ok(())
}
