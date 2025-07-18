fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:example]
    use polars::prelude::*;

    let q = LazyCsvReader::new(PlPath::new("docs/assets/data/iris.csv"))
        .with_has_header(true)
        .finish()?
        .filter(col("sepal_length").gt(lit(5)))
        .group_by(vec![col("species")])
        .agg([col("*").sum()]);

    let df = q.collect()?;
    // --8<-- [end:example]

    println!("{df}");

    Ok(())
}
