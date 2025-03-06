use super::*;

pub(super) fn top_k(args: &[Column], descending: bool) -> PolarsResult<Column> {
    let s = &args[0];
    let k_s = &args[1];
    polars_ensure!(
        k_s.len() == 1,
        ComputeError: "k must be a single value."
    );
    let k_s = k_s.cast(&DataType::Int32)?;
    let k_s = k_s.i32()?;
    let k_opt = k_s.get(0);

    match k_opt {
        Some(k) => s.top_k(k as usize, descending),
        None => polars_bail!(ComputeError: "k must be a single value"),
    }
}
