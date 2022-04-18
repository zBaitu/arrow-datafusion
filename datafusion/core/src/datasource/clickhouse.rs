use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use datafusion_expr::Expr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::error::Result;
use crate::datasource::TableProvider;
use crate::datasource::datasource::TableProviderFilterPushDown;
use crate::physical_plan::clickhouse::MergeTreeExec;
use crate::physical_plan::ExecutionPlan;

pub struct MergeTree {
    schema: SchemaRef,
}

impl MergeTree {
    pub fn new() -> Self {
        Self {
            schema: SchemaRef::new(Schema::new(vec![Field::new("Year", DataType::UInt16, false)])),
        }
    }
}

impl Debug for MergeTree {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
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

    async fn scan(&self, projection: &Option<Vec<usize>>, filters: &[Expr], limit: Option<usize>)
    -> Result<Arc<dyn ExecutionPlan>> {
        println!("{:?}", projection);
        println!("{:?}", filters);
        println!("{:?}", limit);

        Ok(Arc::new(MergeTreeExec::new(self.schema())))
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Exact)
    }
}
