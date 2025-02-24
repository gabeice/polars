use super::*;

pub struct TopKExpr {
    pub(crate) phys_expr: Arc<dyn PhysicalExpr>,
    pub(crate) k: usize,
    pub(crate) descending: bool,
    pub(crate) expr: Expr,
}

impl PhysicalExpr for TopKExpr {
    fn as_expression(&self) -> Option<&Expr> {
        Some(&self.expr)
    }

    fn evaluate(&self, df: &DataFrame, state: &ExecutionState) -> PolarsResult<Column> {
        let series = self.phys_expr.evaluate(df, state)?;
        series.top_k(self.k, self.descending)
    }

    fn evaluate_on_groups<'a>(
        &self,
        df: &DataFrame,
        groups: &'a GroupPositions,
        state: &ExecutionState,
    ) -> PolarsResult<AggregationContext<'a>> {
        let c = self.evaluate(df, state)?;
        Ok(AggregationContext::new(c, Cow::Borrowed(groups), false))
    }

    fn to_field(&self, input_schema: &Schema) -> PolarsResult<Field> {
        self.phys_expr.to_field(input_schema)
    }

    fn is_scalar(&self) -> bool {
        false
    }
}
