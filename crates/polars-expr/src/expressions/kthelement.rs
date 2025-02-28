use polars_core::prelude::*;

use super::*;

pub struct KthElementExpr {
    pub(crate) phys_expr: Arc<dyn PhysicalExpr>,
    pub(crate) k: Arc<dyn PhysicalExpr>,
    pub(crate) expr: Expr,
}

fn extract_k(k: Column, expr: &Expr) -> PolarsResult<usize> {
    k.get(0)
        .unwrap()
        .extract()
        .ok_or_else(|| polars_err!(expr = expr, ComputeError: "unable to k offset from {:?}", k))
}

impl PhysicalExpr for KthElementExpr {
    fn evaluate(&self, df: &DataFrame, state: &ExecutionState) -> PolarsResult<Column> {
        let series = self.phys_expr.evaluate(df, state)?;
        let k = self.k.evaluate(df, state)?;
        let k = extract_k(k, &self.expr)?;
        series.kth_element(k)
    }

    fn evaluate_on_groups<'a>(
        &self,
        _df: &DataFrame,
        _groups: &'a GroupPositions,
        _state: &ExecutionState,
    ) -> PolarsResult<AggregationContext<'a>> {
        polars_bail!(InvalidOperation: "not implemented yet");
    }

    fn to_field(&self, input_schema: &Schema) -> PolarsResult<Field> {
        self.phys_expr.to_field(input_schema)
    }

    fn is_scalar(&self) -> bool {
        true
    }
}
