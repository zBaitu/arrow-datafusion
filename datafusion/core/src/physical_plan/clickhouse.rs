use arrow::array::UInt16Builder;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_physical_expr::PhysicalSortExpr;

use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::physical_plan::memory::MemoryStream;
use crate::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics};

pub struct MergeTreeExec {
    schema: SchemaRef,
}

impl MergeTreeExec {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
        }
    }
}


impl Debug for MergeTreeExec {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl ExecutionPlan for MergeTreeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
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

    async fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        println!("{:?}", partition);

        let mut year = UInt16Builder::new(2);
        year.append_value(2021)?;
        year.append_value(2022)?;

        return Ok(Box::pin(MemoryStream::try_new(
                vec![RecordBatch::try_new(self.schema.clone(), vec![Arc::new(year.finish())])?],
                self.schema.clone(),
                None)?));
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
