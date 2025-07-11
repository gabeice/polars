//! this contains code used for rewriting projections, expanding wildcards, regex selection etc.

use super::*;

pub fn prepare_projection(
    exprs: Vec<Expr>,
    schema: &Schema,
    opt_flags: &mut OptFlags,
) -> PolarsResult<(Vec<Expr>, Schema)> {
    let exprs = rewrite_projections(exprs, schema, &[], opt_flags)?;
    let schema = expressions_to_schema(&exprs, schema, Context::Default)?;
    Ok((exprs, schema))
}

/// This replaces the wildcard Expr with a Column Expr. It also removes the Exclude Expr from the
/// expression chain.
pub(super) fn replace_wildcard_with_column(expr: Expr, column_name: &PlSmallStr) -> Expr {
    expr.map_expr(|e| match e {
        Expr::Wildcard => Expr::Column(column_name.clone()),
        Expr::Exclude(input, _) => Arc::unwrap_or_clone(input),
        e => e,
    })
}

#[cfg(feature = "regex")]
fn remove_exclude(expr: Expr) -> Expr {
    expr.map_expr(|e| match e {
        Expr::Exclude(input, _) => Arc::unwrap_or_clone(input),
        e => e,
    })
}

fn rewrite_special_aliases(expr: Expr) -> PolarsResult<Expr> {
    // the blocks are added by cargo fmt
    if has_expr(&expr, |e| {
        matches!(e, Expr::KeepName(_) | Expr::RenameAlias { .. })
    }) {
        match expr {
            Expr::KeepName(expr) => {
                let roots = expr_to_leaf_column_names(&expr);
                let name = roots
                    .first()
                    .expect("expected root column to keep expression name");
                Ok(Expr::Alias(expr, name.clone()))
            },
            Expr::RenameAlias { expr, function } => {
                let name = get_single_leaf(&expr)?;
                let name = function.call(&name)?;
                Ok(Expr::Alias(expr, name))
            },
            _ => {
                polars_bail!(InvalidOperation: "`keep`, `suffix`, `prefix` should be last expression")
            },
        }
    } else {
        Ok(expr)
    }
}

/// Take an expression with a root: col("*") and copies that expression for all columns in the schema,
/// with the exclusion of the `names` in the exclude expression.
/// The resulting expressions are written to result.
fn replace_wildcard(
    expr: &Expr,
    result: &mut Vec<Expr>,
    exclude: &PlHashSet<PlSmallStr>,
    schema: &Schema,
) -> PolarsResult<()> {
    for name in schema.iter_names() {
        if !exclude.contains(name.as_str()) {
            let new_expr = replace_wildcard_with_column(expr.clone(), name);
            let new_expr = rewrite_special_aliases(new_expr)?;
            result.push(new_expr)
        }
    }
    Ok(())
}

fn replace_nth(expr: Expr, schema: &Schema) -> Expr {
    expr.map_expr(|e| {
        if let Expr::Nth(i) = e {
            match i.negative_to_usize(schema.len()) {
                None => {
                    let name = match i {
                        0 => "first",
                        -1 => "last",
                        _ => "nth",
                    };
                    Expr::Column(PlSmallStr::from_static(name))
                },
                Some(idx) => {
                    let (name, _dtype) = schema.get_at_index(idx).unwrap();
                    Expr::Column(name.clone())
                },
            }
        } else {
            e
        }
    })
}

#[cfg(feature = "regex")]
/// This function takes an expression containing a regex in `col("..")` and expands the columns
/// that are selected by that regex in `result`.
fn expand_regex(
    expr: &Expr,
    result: &mut Vec<Expr>,
    schema: &Schema,
    pattern: &str,
    exclude: &PlHashSet<PlSmallStr>,
) -> PolarsResult<()> {
    let re = polars_utils::regex_cache::compile_regex(pattern)
        .map_err(|e| polars_err!(ComputeError: "invalid regex {}", e))?;
    for name in schema.iter_names() {
        if re.is_match(name) && !exclude.contains(name.as_str()) {
            let mut new_expr = remove_exclude(expr.clone());

            new_expr = new_expr.map_expr(|e| match e {
                Expr::Column(pat) if pat.as_str() == pattern => Expr::Column(name.clone()),
                e => e,
            });

            let new_expr = rewrite_special_aliases(new_expr)?;
            result.push(new_expr);
        }
    }
    Ok(())
}

pub fn is_regex_projection(name: &str) -> bool {
    name.starts_with('^') && name.ends_with('$')
}

#[cfg(feature = "regex")]
/// This function searches for a regex expression in `col("..")` and expands the columns
/// that are selected by that regex in `result`. The regex should start with `^` and end with `$`.
fn replace_regex(
    expr: &Expr,
    result: &mut Vec<Expr>,
    schema: &Schema,
    exclude: &PlHashSet<PlSmallStr>,
) -> PolarsResult<()> {
    let roots = expr_to_leaf_column_names(expr);
    let mut regex = None;
    for name in &roots {
        if is_regex_projection(name) {
            match regex {
                None => {
                    regex = Some(name);
                    expand_regex(expr, result, schema, name, exclude)?;
                },
                Some(r) => {
                    polars_ensure!(
                        r == name,
                        ComputeError:
                        "an expression is not allowed to have different regexes"
                    )
                },
            }
        }
    }
    if regex.is_none() {
        let expr = rewrite_special_aliases(expr.clone())?;
        result.push(expr)
    }
    Ok(())
}

/// replace `columns(["A", "B"])..` with `col("A")..`, `col("B")..`
#[allow(unused_variables)]
fn expand_columns(
    expr: &Expr,
    result: &mut Vec<Expr>,
    names: &[PlSmallStr],
    schema: &Schema,
    exclude: &PlHashSet<PlSmallStr>,
) -> PolarsResult<()> {
    if !expr.into_iter().all(|e| match e {
        // check for invalid expansions such as `col([a, b]) + col([c, d])`
        Expr::Columns(members) => members.as_ref() == names,
        _ => true,
    }) {
        polars_bail!(ComputeError: "expanding more than one `col` is not allowed");
    }
    for name in names {
        if !exclude.contains(name) {
            let new_expr = expr.clone().map_expr(|e| match e {
                Expr::Columns(_) => Expr::Column((*name).clone()),
                Expr::Exclude(input, _) => Arc::unwrap_or_clone(input),
                e => e,
            });

            #[cfg(feature = "regex")]
            replace_regex(&new_expr, result, schema, exclude)?;

            #[cfg(not(feature = "regex"))]
            result.push(rewrite_special_aliases(new_expr)?);
        }
    }
    Ok(())
}

#[cfg(feature = "dtype-struct")]
fn struct_index_to_field(expr: Expr, schema: &Schema) -> PolarsResult<Expr> {
    expr.try_map_expr(|e| match e {
        Expr::Function {
            input,
            function: FunctionExpr::StructExpr(sf),
        } => {
            if let StructFunction::FieldByIndex(index) = sf {
                let dtype = input[0].to_field(schema, Context::Default)?.dtype;
                let DataType::Struct(fields) = dtype else {
                    polars_bail!(InvalidOperation: "expected 'struct' dtype, got {:?}", dtype)
                };
                let index = index.try_negative_to_usize(fields.len())?;
                let name = fields[index].name.clone();
                Ok(Expr::Function {
                    input,
                    function: FunctionExpr::StructExpr(StructFunction::FieldByName(name)),
                })
            } else {
                Ok(Expr::Function {
                    input,
                    function: FunctionExpr::StructExpr(sf),
                })
            }
        },
        e => Ok(e),
    })
}

/// This replaces the dtype or index expanded Expr with a Column Expr.
/// ()It also removes the Exclude Expr from the expression chain).
fn replace_dtype_or_index_with_column(
    expr: Expr,
    column_name: &PlSmallStr,
    replace_dtype: bool,
) -> Expr {
    expr.map_expr(|e| match e {
        Expr::DtypeColumn(_) if replace_dtype => Expr::Column(column_name.clone()),
        Expr::IndexColumn(_) if !replace_dtype => Expr::Column(column_name.clone()),
        Expr::Exclude(input, _) => Arc::unwrap_or_clone(input),
        e => e,
    })
}

fn dtypes_match(d1: &DataType, d2: &DataType) -> bool {
    match (d1, d2) {
        // note: allow Datetime "*" wildcard for timezones...
        (DataType::Datetime(tu_l, tz_l), DataType::Datetime(tu_r, tz_r)) => {
            tu_l == tu_r
                && (tz_l == tz_r
                    || match (tz_l, tz_r) {
                        (Some(l), Some(r)) => TimeZone::eq_wildcard_aware(l, r),
                        _ => false,
                    })
        },
        // ...but otherwise require exact match
        _ => d1 == d2,
    }
}

/// replace `DtypeColumn` with `col("foo")..col("bar")`
fn expand_dtypes(
    expr: &Expr,
    result: &mut Vec<Expr>,
    schema: &Schema,
    dtypes: &[DataType],
    exclude: &PlHashSet<PlSmallStr>,
) -> PolarsResult<()> {
    // note: we loop over the schema to guarantee that we return a stable
    // field-order, irrespective of which dtypes are filtered against
    for field in schema.iter_fields().filter(|f| {
        dtypes.iter().any(|dtype| dtypes_match(dtype, &f.dtype))
            && !exclude.contains(f.name().as_str())
    }) {
        let name = field.name();
        let new_expr = expr.clone();
        let new_expr = replace_dtype_or_index_with_column(new_expr, name, true);
        let new_expr = rewrite_special_aliases(new_expr)?;
        result.push(new_expr)
    }
    Ok(())
}

#[cfg(feature = "dtype-struct")]
fn replace_struct_multiple_fields_with_field(
    expr: Expr,
    column_name: &PlSmallStr,
) -> PolarsResult<Expr> {
    let mut count = 0;
    let out = expr.map_expr(|e| match e {
        Expr::Function { function, input } => {
            if matches!(
                function,
                FunctionExpr::StructExpr(StructFunction::MultipleFields(_))
            ) {
                count += 1;
                Expr::Function {
                    input,
                    function: FunctionExpr::StructExpr(StructFunction::FieldByName(
                        column_name.clone(),
                    )),
                }
            } else {
                Expr::Function { input, function }
            }
        },
        e => e,
    });
    polars_ensure!(count == 1, InvalidOperation: "multiple expanding fields in a single struct not yet supported");
    Ok(out)
}

#[cfg(feature = "dtype-struct")]
fn expand_struct_fields(
    struct_expr: &Expr,
    full_expr: &Expr,
    result: &mut Vec<Expr>,
    schema: &Schema,
    names: &[PlSmallStr],
    exclude: &PlHashSet<PlSmallStr>,
) -> PolarsResult<()> {
    let Some(first_name) = names.first() else {
        return Ok(());
    };
    if names.len() == 1 && first_name == "*" || is_regex_projection(first_name) {
        let Expr::Function { input, .. } = struct_expr else {
            unreachable!()
        };
        let field = input[0].to_field(schema, Context::Default)?;
        let dtype = field.dtype();
        let DataType::Struct(fields) = dtype else {
            if !dtype.is_known() {
                let mut msg = String::from(
                    "expected 'struct' got an unknown data type

This means there was an operation of which the output data type could not be determined statically.
Try setting the output data type for that operation.",
                );
                for e in input[0].into_iter() {
                    #[allow(clippy::single_match)]
                    match e {
                        #[cfg(feature = "list_to_struct")]
                        Expr::Function { input: _, function } => {
                            if matches!(
                                function,
                                FunctionExpr::ListExpr(ListFunction::ToStruct(..))
                            ) {
                                msg.push_str(
                                    "

Hint: set 'upper_bound' for 'list.to_struct'.",
                                );
                            }
                        },
                        _ => {},
                    }
                }

                polars_bail!(InvalidOperation: msg)
            } else {
                polars_bail!(InvalidOperation: "expected 'struct' got {}", field.dtype())
            }
        };

        // Wildcard.
        let names = if first_name == "*" {
            fields
                .iter()
                .flat_map(|field| {
                    let name = field.name();

                    if exclude.contains(name.as_str()) {
                        None
                    } else {
                        Some(name.clone())
                    }
                })
                .collect::<Vec<_>>()
        }
        // Regex
        else {
            #[cfg(feature = "regex")]
            {
                let re = polars_utils::regex_cache::compile_regex(first_name)
                    .map_err(|e| polars_err!(ComputeError: "invalid regex {}", e))?;

                fields
                    .iter()
                    .flat_map(|field| {
                        let name = field.name();
                        if exclude.contains(name.as_str()) || !re.is_match(name.as_str()) {
                            None
                        } else {
                            Some(name.clone())
                        }
                    })
                    .collect::<Vec<_>>()
            }
            #[cfg(not(feature = "regex"))]
            {
                panic!("activate 'regex' feature")
            }
        };

        return expand_struct_fields(
            struct_expr,
            full_expr,
            result,
            schema,
            names.as_slice(),
            exclude,
        );
    }

    for name in names {
        polars_ensure!(name.as_str() != "*", InvalidOperation: "cannot combine wildcards and column names");

        if !exclude.contains(name) {
            let mut new_expr = replace_struct_multiple_fields_with_field(full_expr.clone(), name)?;
            match new_expr {
                Expr::KeepName(expr) => {
                    new_expr = Expr::Alias(expr, name.clone());
                },
                Expr::RenameAlias { expr, function } => {
                    let name = function.call(name)?;
                    new_expr = Expr::Alias(expr, name);
                },
                _ => {},
            }

            result.push(new_expr)
        }
    }
    Ok(())
}

/// replace `IndexColumn` with `col("foo")..col("bar")`
fn expand_indices(
    expr: &Expr,
    result: &mut Vec<Expr>,
    schema: &Schema,
    indices: &[i64],
    exclude: &PlHashSet<PlSmallStr>,
) -> PolarsResult<()> {
    let n_fields = schema.len() as i64;
    for idx in indices {
        let mut idx = *idx;
        if idx < 0 {
            idx += n_fields;
            if idx < 0 {
                polars_bail!(ComputeError: "invalid column index {}", idx)
            }
        }
        if let Some((name, _)) = schema.get_at_index(idx as usize) {
            if !exclude.contains(name.as_str()) {
                let new_expr = expr.clone();
                let new_expr = replace_dtype_or_index_with_column(new_expr, name, false);
                let new_expr = rewrite_special_aliases(new_expr)?;
                result.push(new_expr);
            }
        }
    }
    Ok(())
}

// schema is not used if regex not activated
#[allow(unused_variables)]
fn prepare_excluded(
    expr: &Expr,
    schema: &Schema,
    keys: &[Expr],
    has_exclude: bool,
) -> PolarsResult<PlHashSet<PlSmallStr>> {
    let mut exclude = PlHashSet::new();

    // explicit exclude branch
    if has_exclude {
        for e in expr {
            if let Expr::Exclude(_, to_exclude) = e {
                #[cfg(feature = "regex")]
                {
                    // instead of matching the names for regex patterns and
                    // expanding the matches in the schema we reuse the
                    // `replace_regex` func; this is a bit slower but DRY.
                    let mut buf = vec![];
                    for to_exclude_single in to_exclude {
                        match to_exclude_single {
                            Excluded::Name(name) => {
                                let e = Expr::Column(name.clone());
                                replace_regex(&e, &mut buf, schema, &Default::default())?;
                                // we cannot loop because of bchck
                                while let Some(col) = buf.pop() {
                                    if let Expr::Column(name) = col {
                                        exclude.insert(name);
                                    }
                                }
                            },
                            Excluded::Dtype(dt) => {
                                for fld in schema.iter_fields() {
                                    if dtypes_match(fld.dtype(), dt) {
                                        exclude.insert(fld.name.clone());
                                    }
                                }
                            },
                        }
                    }
                }

                #[cfg(not(feature = "regex"))]
                {
                    for to_exclude_single in to_exclude {
                        match to_exclude_single {
                            Excluded::Name(name) => {
                                exclude.insert(name.clone());
                            },
                            Excluded::Dtype(dt) => {
                                for (name, dtype) in schema.iter() {
                                    if matches!(dtype, dt) {
                                        exclude.insert(name.clone());
                                    }
                                }
                            },
                        }
                    }
                }
            }
        }
    }

    // exclude group_by keys
    for expr in keys.iter() {
        if let Ok(name) = expr_output_name(expr) {
            exclude.insert(name.clone());
        }
    }
    Ok(exclude)
}

// functions can have col(["a", "b"]) or col(String) as inputs
fn expand_function_inputs(
    expr: Expr,
    schema: &Schema,
    opt_flags: &mut OptFlags,
) -> PolarsResult<Expr> {
    expr.try_map_expr(|mut e| match &mut e {
        Expr::Function {
            input, function, ..
        } => {
            use FunctionExpr as F;
            let mut input_wildcard_expansion = matches!(function, F::Boolean(BooleanFunction::AnyHorizontal | BooleanFunction::AllHorizontal)
                | F::Coalesce
                | F::ListExpr(ListFunction::Concat)
                | F::ConcatExpr(_)
                | F::MinHorizontal
                | F::MaxHorizontal
                | F::SumHorizontal { .. }
                | F::MeanHorizontal { .. }
            );
            let mut allow_empty_inputs = matches!(
                function,
                F::Boolean(BooleanFunction::AnyHorizontal | BooleanFunction::AllHorizontal)
                | F::DropNulls
            );
            #[cfg(feature = "dtype-array")]
            {
                input_wildcard_expansion |= matches!(function, F::ArrayExpr(ArrayFunction::Concat));
            }
            #[cfg(feature = "dtype-struct")]
            {
                input_wildcard_expansion |= matches!(function, F::AsStruct);
                input_wildcard_expansion |= matches!( function, F::StructExpr(StructFunction::WithFields));
            }
            #[cfg(feature = "ffi_plugin")]
            {
                input_wildcard_expansion |= matches!(function, F::FfiPlugin { flags, .. } if flags.flags.contains(FunctionFlags::INPUT_WILDCARD_EXPANSION));
                allow_empty_inputs |= matches!(function, F::FfiPlugin { flags, .. } if flags.flags.contains(FunctionFlags::ALLOW_EMPTY_INPUTS));
            }
            #[cfg(feature = "concat_str")]
            {
                input_wildcard_expansion |= matches!(function, F::StringExpr(StringFunction::ConcatHorizontal { .. }));
            }

            if input_wildcard_expansion {
                *input = rewrite_projections(core::mem::take(input), schema, &[], opt_flags)?;
                if input.is_empty() && !allow_empty_inputs {
                    // Needed to visualize the error
                    *input = vec![Expr::Literal(LiteralValue::Scalar(Scalar::null(
                        DataType::Null,
                    )))];
                    polars_bail!(InvalidOperation: "expected at least 1 input in {}", e)
                }
            }
            Ok(e)
        },
        Expr::AnonymousFunction { input, options, .. } if options.flags.contains(FunctionFlags::INPUT_WILDCARD_EXPANSION) => {
            *input = rewrite_projections(core::mem::take(input), schema, &[], opt_flags)?;
            if input.is_empty() && !options.flags.contains(FunctionFlags::ALLOW_EMPTY_INPUTS) {
                // Needed to visualize the error
                *input = vec![Expr::Literal(LiteralValue::Scalar(Scalar::null(
                    DataType::Null,
                )))];
                polars_bail!(InvalidOperation: "expected at least 1 input in {}", e)
            }
            Ok(e)
        },
        _ => Ok(e),
    })
}

#[derive(Copy, Clone, Debug)]
struct ExpansionFlags {
    multiple_columns: bool,
    has_nth: bool,
    has_wildcard: bool,
    has_selector: bool,
    has_exclude: bool,
    #[cfg(feature = "dtype-struct")]
    expands_fields: bool,
    #[cfg(feature = "dtype-struct")]
    has_struct_field_by_index: bool,
}

impl ExpansionFlags {
    fn expands(&self) -> bool {
        #[cfg(feature = "dtype-struct")]
        let expands_fields = self.expands_fields;
        #[cfg(not(feature = "dtype-struct"))]
        let expands_fields = false;

        self.multiple_columns || expands_fields
    }
}

fn find_flags(expr: &Expr) -> PolarsResult<ExpansionFlags> {
    let mut multiple_columns = false;
    let mut has_nth = false;
    let mut has_wildcard = false;
    let mut has_selector = false;
    let mut has_exclude = false;
    #[cfg(feature = "dtype-struct")]
    let mut has_struct_field_by_index = false;
    #[cfg(feature = "dtype-struct")]
    let mut expands_fields = false;

    // Do a single pass and collect all flags at once.
    // Supertypes/modification that can be done in place are also done in that pass
    for expr in expr {
        match expr {
            Expr::Columns(_) | Expr::DtypeColumn(_) => multiple_columns = true,
            Expr::IndexColumn(idx) => multiple_columns = idx.len() > 1,
            Expr::Nth(_) => has_nth = true,
            Expr::Wildcard => has_wildcard = true,
            Expr::Selector(_) => has_selector = true,
            #[cfg(feature = "dtype-struct")]
            Expr::Function {
                function: FunctionExpr::StructExpr(StructFunction::FieldByIndex(_)),
                ..
            } => {
                has_struct_field_by_index = true;
            },
            #[cfg(feature = "dtype-struct")]
            Expr::Function {
                function: FunctionExpr::StructExpr(StructFunction::MultipleFields(_)),
                ..
            } => {
                expands_fields = true;
            },
            Expr::Exclude(_, _) => has_exclude = true,
            #[cfg(feature = "dtype-struct")]
            Expr::Field(_) => {
                polars_bail!(InvalidOperation: "field expression not allowed at location/context")
            },
            _ => {},
        }
    }
    Ok(ExpansionFlags {
        multiple_columns,
        has_nth,
        has_wildcard,
        has_selector,
        has_exclude,
        #[cfg(feature = "dtype-struct")]
        has_struct_field_by_index,
        #[cfg(feature = "dtype-struct")]
        expands_fields,
    })
}

#[cfg(feature = "dtype-struct")]
fn toggle_cse(opt_flags: &mut OptFlags) {
    if opt_flags.contains(OptFlags::EAGER) && !opt_flags.contains(OptFlags::NEW_STREAMING) {
        #[cfg(debug_assertions)]
        {
            use polars_core::config::verbose;
            if verbose() {
                eprintln!("CSE turned on because of struct expansion")
            }
        }
        *opt_flags |= OptFlags::COMM_SUBEXPR_ELIM;
    }
}

/// In case of single col(*) -> do nothing, no selection is the same as select all
/// In other cases replace the wildcard with an expression with all columns
pub fn rewrite_projections(
    exprs: Vec<Expr>,
    schema: &Schema,
    keys: &[Expr],
    opt_flags: &mut OptFlags,
) -> PolarsResult<Vec<Expr>> {
    let mut result = Vec::with_capacity(exprs.len() + schema.len());

    for mut expr in exprs {
        #[cfg(feature = "dtype-struct")]
        let result_offset = result.len();

        // Functions can have col(["a", "b"]) or col(String) as inputs.
        expr = expand_function_inputs(expr, schema, opt_flags)?;

        let mut flags = find_flags(&expr)?;
        if flags.has_selector {
            expr = replace_selector(expr, schema, keys)?;
            // the selector is replaced with Expr::Columns
            flags.multiple_columns = true;
        }

        replace_and_add_to_results(expr, flags, &mut result, schema, keys, opt_flags)?;

        #[cfg(feature = "dtype-struct")]
        if flags.has_struct_field_by_index {
            toggle_cse(opt_flags);
            for e in &mut result[result_offset..] {
                *e = struct_index_to_field(std::mem::take(e), schema)?;
            }
        }
    }
    Ok(result)
}

fn replace_and_add_to_results(
    mut expr: Expr,
    flags: ExpansionFlags,
    result: &mut Vec<Expr>,
    schema: &Schema,
    keys: &[Expr],
    opt_flags: &mut OptFlags,
) -> PolarsResult<()> {
    if flags.has_nth {
        expr = replace_nth(expr, schema);
    }

    // has multiple column names
    // the expanded columns are added to the result
    if flags.expands() {
        if let Some(e) = expr.into_iter().find(|e| match e {
            Expr::Columns(_) | Expr::DtypeColumn(_) | Expr::IndexColumn(_) => true,
            #[cfg(feature = "dtype-struct")]
            Expr::Function {
                function: FunctionExpr::StructExpr(StructFunction::MultipleFields(_)),
                ..
            } => flags.expands_fields,
            _ => false,
        }) {
            match &e {
                Expr::Columns(names) => {
                    // Don't exclude grouping keys if columns are explicitly specified.
                    let exclude = prepare_excluded(&expr, schema, &[], flags.has_exclude)?;
                    expand_columns(&expr, result, names, schema, &exclude)?;
                },
                Expr::DtypeColumn(dtypes) => {
                    let exclude = prepare_excluded(&expr, schema, keys, flags.has_exclude)?;
                    expand_dtypes(&expr, result, schema, dtypes, &exclude)?
                },
                Expr::IndexColumn(indices) => {
                    let exclude = prepare_excluded(&expr, schema, keys, flags.has_exclude)?;
                    expand_indices(&expr, result, schema, indices, &exclude)?
                },
                #[cfg(feature = "dtype-struct")]
                Expr::Function { function, .. } => {
                    let FunctionExpr::StructExpr(StructFunction::MultipleFields(names)) = function
                    else {
                        unreachable!()
                    };
                    let exclude = prepare_excluded(&expr, schema, keys, flags.has_exclude)?;

                    // has both column and field expansion
                    // col('a', 'b').struct.field('*')
                    if flags.multiple_columns | flags.has_wildcard {
                        // First expand col('a', 'b') into an intermediate result.
                        let mut intermediate = vec![];
                        let mut flags = flags;
                        flags.expands_fields = false;
                        replace_and_add_to_results(
                            expr.clone(),
                            flags,
                            &mut intermediate,
                            schema,
                            keys,
                            opt_flags,
                        )?;

                        // Then expand the fields and add to the final result vec.
                        flags.expands_fields = true;
                        flags.multiple_columns = false;
                        flags.has_wildcard = false;
                        for e in intermediate {
                            replace_and_add_to_results(e, flags, result, schema, keys, opt_flags)?;
                        }
                    }
                    // has only field expansion
                    // col('a').struct.field('*')
                    else {
                        toggle_cse(opt_flags);
                        expand_struct_fields(e, &expr, result, schema, names, &exclude)?
                    }
                },
                _ => {},
            }
        }
    }
    // has multiple column names due to wildcards
    else if flags.has_wildcard {
        // keep track of column excluded from the wildcard
        let exclude = prepare_excluded(&expr, schema, keys, flags.has_exclude)?;
        // this path prepares the wildcard as input for the Function Expr
        replace_wildcard(&expr, result, &exclude, schema)?;
    }
    // can have multiple column names due to a regex
    else {
        #[allow(clippy::collapsible_else_if)]
        #[cfg(feature = "regex")]
        {
            // keep track of column excluded from the dtypes
            let exclude = prepare_excluded(&expr, schema, keys, flags.has_exclude)?;
            replace_regex(&expr, result, schema, &exclude)?;
        }
        #[cfg(not(feature = "regex"))]
        {
            let expr = rewrite_special_aliases(expr)?;
            result.push(expr)
        }
    }
    Ok(())
}

fn replace_selector_inner(
    s: Selector,
    members: &mut PlIndexSet<Expr>,
    scratch: &mut Vec<Expr>,
    schema: &Schema,
    keys: &[Expr],
) -> PolarsResult<()> {
    match s {
        Selector::Root(expr) => {
            let local_flags = find_flags(&expr)?;
            replace_and_add_to_results(
                *expr,
                local_flags,
                scratch,
                schema,
                keys,
                &mut Default::default(),
            )?;
            members.extend(scratch.drain(..))
        },
        Selector::Add(lhs, rhs) => {
            let mut tmp_members: PlIndexSet<Expr> = Default::default();
            replace_selector_inner(*lhs, members, scratch, schema, keys)?;
            replace_selector_inner(*rhs, &mut tmp_members, scratch, schema, keys)?;
            members.extend(tmp_members)
        },
        Selector::ExclusiveOr(lhs, rhs) => {
            let mut tmp_members = Default::default();
            replace_selector_inner(*lhs, &mut tmp_members, scratch, schema, keys)?;
            replace_selector_inner(*rhs, members, scratch, schema, keys)?;

            *members = tmp_members.symmetric_difference(members).cloned().collect();
        },
        Selector::Intersect(lhs, rhs) => {
            let mut tmp_members = Default::default();
            replace_selector_inner(*lhs, &mut tmp_members, scratch, schema, keys)?;
            replace_selector_inner(*rhs, members, scratch, schema, keys)?;

            *members = tmp_members.intersection(members).cloned().collect();
        },
        Selector::Sub(lhs, rhs) => {
            let mut tmp_members = Default::default();
            replace_selector_inner(*lhs, &mut tmp_members, scratch, schema, keys)?;
            replace_selector_inner(*rhs, members, scratch, schema, keys)?;

            *members = tmp_members.difference(members).cloned().collect();
        },
    }
    Ok(())
}

fn replace_selector(expr: Expr, schema: &Schema, keys: &[Expr]) -> PolarsResult<Expr> {
    // First pass we replace the selectors with Expr::Columns, we expand the `to_add` columns
    // and then subtract the `to_subtract` columns.
    expr.try_map_expr(|e| match e {
        Expr::Selector(mut s) => {
            let mut swapped = Selector::Root(Box::new(Expr::Wildcard));
            std::mem::swap(&mut s, &mut swapped);

            let cols = expand_selector(swapped, schema, keys)?;
            Ok(Expr::Columns(cols))
        },
        e => Ok(e),
    })
}

pub fn expand_selectors(
    s: Vec<Selector>,
    schema: &Schema,
    keys: &[Expr],
) -> PolarsResult<Arc<[PlSmallStr]>> {
    let mut columns = vec![];

    // Skip the column fast paths.
    fn skip(name: &str) -> bool {
        is_regex_projection(name) || name == "*"
    }

    for s in s {
        match s {
            Selector::Root(e) => match *e {
                Expr::Column(name) if !skip(name.as_ref()) => columns.push(name),
                Expr::Columns(names) if names.iter().all(|n| !skip(n.as_ref())) => {
                    columns.extend_from_slice(names.as_ref())
                },
                Expr::Selector(s) => {
                    let names = expand_selector(s, schema, keys)?;
                    columns.extend_from_slice(names.as_ref());
                },
                e => {
                    let names = expand_selector(Selector::new(e), schema, keys)?;
                    columns.extend_from_slice(names.as_ref());
                },
            },
            other => {
                let names = expand_selector(other, schema, keys)?;
                columns.extend_from_slice(names.as_ref());
            },
        }
    }

    Ok(Arc::from(columns))
}

pub(super) fn expand_selector(
    s: Selector,
    schema: &Schema,
    keys: &[Expr],
) -> PolarsResult<Arc<[PlSmallStr]>> {
    let mut members = PlIndexSet::new();
    replace_selector_inner(s, &mut members, &mut vec![], schema, keys)?;

    if members.len() <= 1 {
        members
            .into_iter()
            .map(|e| {
                let Expr::Column(name) = e else {
                    polars_bail!(InvalidOperation: "invalid selector expression: {}", e)
                };
                Ok(name)
            })
            .collect()
    } else {
        // Ensure that multiple columns returned from combined/nested selectors remain in schema order
        let selected = schema
            .iter_fields()
            .map(|field| field.name().clone())
            .filter(|field_name| members.contains(&Expr::Column(field_name.clone())))
            .collect();

        Ok(selected)
    }
}
