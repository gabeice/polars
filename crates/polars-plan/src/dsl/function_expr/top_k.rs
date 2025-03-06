use super::*;

pub(super) fn top_k(s: &Column, k: usize, descending: bool) -> PolarsResult<Column> {
    s.top_k(k, descending)
}
