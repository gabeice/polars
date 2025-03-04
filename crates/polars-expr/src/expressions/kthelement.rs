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
        df: &DataFrame,
        groups: &'a GroupPositions,
        state: &ExecutionState,
    ) -> PolarsResult<AggregationContext<'a>> {
        let mut ac = self.phys_expr.evaluate_on_groups(df, groups, state)?;
        let k = self.k.evaluate(df, state)?;
        let k = extract_k(k, &self.expr)?;
        match ac.agg_state() {
            AggState::AggregatedList(s) => {
                let out = s.kth_element(k)?;
                ac.with_values(out.into_column(), true, Some(&self.expr))?;
            },
            _ => {
                polars_bail!(InvalidOperation: "not implemented");
            },
        }

        Ok(ac)
    }

    fn to_field(&self, input_schema: &Schema) -> PolarsResult<Field> {
        self.phys_expr.to_field(input_schema)
    }

    fn is_scalar(&self) -> bool {
        true
    }
}
