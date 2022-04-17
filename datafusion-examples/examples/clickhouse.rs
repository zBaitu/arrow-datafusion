use async_trait::async_trait;
use datafusion::arrow::array::{UInt16Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics};
use datafusion::prelude::SessionContext;

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

pub struct MergeTree {
    schema: SchemaRef,
}

impl MergeTree {
    fn new() -> Self {
        Self {
            schema: SchemaRef::new(Schema::new(vec![Field::new("Year", DataType::UInt16, false)])),
        }
    }
}

impl Debug for MergeTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl ExecutionPlan for MergeTree {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    async fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let mut year = UInt16Builder::new(1);
        year.append_value(2022);

        return Ok(Box::pin(MemoryStream::try_new(
                vec![RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(year.finish())])?],
                self.schema.clone(),
                None)?));
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[async_trait]
impl TableProvider for MergeTree {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(&self, projection: &Option<Vec<usize>>, _filters: &[Expr], _limit: Option<usize>)
    -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MergeTree::new()))
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("ontime", Arc::new(MergeTree::new()))?;

    let df = ctx.sql("SELECT count(*) FROM ontime;").await?;
    df.show().await?;
    Ok(())
}
