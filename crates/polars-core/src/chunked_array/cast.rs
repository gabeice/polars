//! Implementations of the ChunkCast Trait.

use polars_compute::cast::CastOptionsImpl;
#[cfg(feature = "serde-lazy")]
use serde::{Deserialize, Serialize};

use super::flags::StatisticsFlags;
#[cfg(feature = "timezones")]
use crate::chunked_array::temporal::validate_time_zone;
#[cfg(feature = "dtype-datetime")]
use crate::prelude::DataType::Datetime;
use crate::prelude::*;

#[derive(Copy, Clone, Debug, Default, PartialEq, Hash, Eq)]
#[cfg_attr(feature = "serde-lazy", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum CastOptions {
    /// Raises on overflow
    #[default]
    Strict,
    /// Overflow is replaced with null
    NonStrict,
    /// Allows wrapping overflow
    Overflowing,
}

impl CastOptions {
    pub fn strict(&self) -> bool {
        matches!(self, CastOptions::Strict)
    }
}

impl From<CastOptions> for CastOptionsImpl {
    fn from(value: CastOptions) -> Self {
        let wrapped = match value {
            CastOptions::Strict | CastOptions::NonStrict => false,
            CastOptions::Overflowing => true,
        };
        CastOptionsImpl {
            wrapped,
            partial: false,
        }
    }
}

pub(crate) fn cast_chunks(
    chunks: &[ArrayRef],
    dtype: &DataType,
    options: CastOptions,
) -> PolarsResult<Vec<ArrayRef>> {
    let check_nulls = matches!(options, CastOptions::Strict);
    let options = options.into();

    let arrow_dtype = dtype.try_to_arrow(CompatLevel::newest())?;
    chunks
        .iter()
        .map(|arr| {
            let out = polars_compute::cast::cast(arr.as_ref(), &arrow_dtype, options);
            if check_nulls {
                out.and_then(|new| {
                    polars_ensure!(arr.null_count() == new.null_count(), ComputeError: "strict cast failed");
                    Ok(new)
                })

            } else {
                out
            }
        })
        .collect::<PolarsResult<Vec<_>>>()
}

fn cast_impl_inner(
    name: PlSmallStr,
    chunks: &[ArrayRef],
    dtype: &DataType,
    options: CastOptions,
) -> PolarsResult<Series> {
    let chunks = match dtype {
        #[cfg(feature = "dtype-decimal")]
        DataType::Decimal(_, _) => {
            let mut chunks = cast_chunks(chunks, dtype, options)?;
            // @NOTE: We cannot cast here as that will lower the scale.
            for chunk in chunks.iter_mut() {
                *chunk = std::mem::take(
                    chunk
                        .as_any_mut()
                        .downcast_mut::<PrimitiveArray<i128>>()
                        .unwrap(),
                )
                .to(ArrowDataType::Int128)
                .to_boxed();
            }
            chunks
        },
        _ => cast_chunks(chunks, &dtype.to_physical(), options)?,
    };

    let out = Series::try_from((name, chunks))?;
    use DataType::*;
    let out = match dtype {
        Date => out.into_date(),
        Datetime(tu, tz) => match tz {
            #[cfg(feature = "timezones")]
            Some(tz) => {
                validate_time_zone(tz)?;
                out.into_datetime(*tu, Some(tz.clone()))
            },
            _ => out.into_datetime(*tu, None),
        },
        Duration(tu) => out.into_duration(*tu),
        #[cfg(feature = "dtype-time")]
        Time => out.into_time(),
        #[cfg(feature = "dtype-decimal")]
        Decimal(precision, scale) => out.into_decimal(*precision, scale.unwrap_or(0))?,
        _ => out,
    };

    Ok(out)
}

fn cast_impl(
    name: PlSmallStr,
    chunks: &[ArrayRef],
    dtype: &DataType,
    options: CastOptions,
) -> PolarsResult<Series> {
    cast_impl_inner(name, chunks, dtype, options)
}

#[cfg(feature = "dtype-struct")]
fn cast_single_to_struct(
    name: PlSmallStr,
    chunks: &[ArrayRef],
    fields: &[Field],
    options: CastOptions,
) -> PolarsResult<Series> {
    polars_ensure!(fields.len() == 1, InvalidOperation: "must specify one field in the struct");
    let mut new_fields = Vec::with_capacity(fields.len());
    // cast to first field dtype
    let mut fields = fields.iter();
    let fld = fields.next().unwrap();
    let s = cast_impl_inner(fld.name.clone(), chunks, &fld.dtype, options)?;
    let length = s.len();
    new_fields.push(s);

    for fld in fields {
        new_fields.push(Series::full_null(fld.name.clone(), length, &fld.dtype));
    }

    StructChunked::from_series(name, length, new_fields.iter()).map(|ca| ca.into_series())
}

impl<T> ChunkedArray<T>
where
    T: PolarsNumericType,
{
    fn cast_impl(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        if self.dtype() == dtype {
            // SAFETY: chunks are correct dtype
            let mut out = unsafe {
                Series::from_chunks_and_dtype_unchecked(
                    self.name().clone(),
                    self.chunks.clone(),
                    dtype,
                )
            };
            out.set_sorted_flag(self.is_sorted_flag());
            return Ok(out);
        }
        match dtype {
            #[cfg(feature = "dtype-categorical")]
            DataType::Categorical(_, ordering) => {
                polars_ensure!(
                    self.dtype() == &DataType::UInt32,
                    ComputeError: "cannot cast numeric types to 'Categorical'"
                );
                // SAFETY:
                // we are guarded by the type system
                let ca = unsafe { &*(self as *const ChunkedArray<T> as *const UInt32Chunked) };

                CategoricalChunked::from_global_indices(ca.clone(), *ordering)
                    .map(|ca| ca.into_series())
            },
            #[cfg(feature = "dtype-categorical")]
            DataType::Enum(rev_map, ordering) => {
                let ca = match self.dtype() {
                    DataType::UInt32 => {
                        // SAFETY: we are guarded by the type system
                        unsafe { &*(self as *const ChunkedArray<T> as *const UInt32Chunked) }
                            .clone()
                    },
                    dt if dt.is_integer() => self
                        .cast_with_options(self.dtype(), options)?
                        .strict_cast(&DataType::UInt32)?
                        .u32()?
                        .clone(),
                    _ => {
                        polars_bail!(ComputeError: "cannot cast non integer types to 'Enum'")
                    },
                };
                let Some(rev_map) = rev_map else {
                    polars_bail!(ComputeError: "cannot cast to Enum without categories");
                };
                let categories = rev_map.get_categories();
                // Check if indices are in bounds
                if let Some(m) = ChunkAgg::max(&ca) {
                    if m >= categories.len() as u32 {
                        polars_bail!(OutOfBounds: "index {} is bigger than the number of categories {}",m,categories.len());
                    }
                }
                // SAFETY: indices are in bound
                unsafe {
                    Ok(CategoricalChunked::from_cats_and_rev_map_unchecked(
                        ca.clone(),
                        rev_map.clone(),
                        true,
                        *ordering,
                    )
                    .into_series())
                }
            },
            #[cfg(feature = "dtype-struct")]
            DataType::Struct(fields) => {
                cast_single_to_struct(self.name().clone(), &self.chunks, fields, options)
            },
            _ => cast_impl_inner(self.name().clone(), &self.chunks, dtype, options).map(|mut s| {
                // maintain sorted if data types
                // - remain signed
                // - unsigned -> signed
                // this may still fail with overflow?
                let to_signed = dtype.is_signed_integer();
                let unsigned2unsigned =
                    self.dtype().is_unsigned_integer() && dtype.is_unsigned_integer();
                let allowed = to_signed || unsigned2unsigned;

                if (allowed)
                    && (s.null_count() == self.null_count())
                    // physical to logicals
                    || (self.dtype().to_physical() == dtype.to_physical())
                {
                    let is_sorted = self.is_sorted_flag();
                    s.set_sorted_flag(is_sorted)
                }
                s
            }),
        }
    }
}

impl<T> ChunkCast for ChunkedArray<T>
where
    T: PolarsNumericType,
{
    fn cast_with_options(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        self.cast_impl(dtype, options)
    }

    unsafe fn cast_unchecked(&self, dtype: &DataType) -> PolarsResult<Series> {
        match dtype {
            #[cfg(feature = "dtype-categorical")]
            DataType::Categorical(Some(rev_map), ordering)
            | DataType::Enum(Some(rev_map), ordering) => {
                if self.dtype() == &DataType::UInt32 {
                    // SAFETY:
                    // we are guarded by the type system.
                    let ca = unsafe { &*(self as *const ChunkedArray<T> as *const UInt32Chunked) };
                    Ok(unsafe {
                        CategoricalChunked::from_cats_and_rev_map_unchecked(
                            ca.clone(),
                            rev_map.clone(),
                            matches!(dtype, DataType::Enum(_, _)),
                            *ordering,
                        )
                    }
                    .into_series())
                } else {
                    polars_bail!(ComputeError: "cannot cast numeric types to 'Categorical'");
                }
            },
            _ => self.cast_impl(dtype, CastOptions::Overflowing),
        }
    }
}

impl ChunkCast for StringChunked {
    fn cast_with_options(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        match dtype {
            #[cfg(feature = "dtype-categorical")]
            DataType::Categorical(rev_map, ordering) => match rev_map {
                None => {
                    // SAFETY: length is correct
                    let iter =
                        unsafe { self.downcast_iter().flatten().trust_my_length(self.len()) };
                    let builder =
                        CategoricalChunkedBuilder::new(self.name().clone(), self.len(), *ordering);
                    let ca = builder.drain_iter_and_finish(iter);
                    Ok(ca.into_series())
                },
                Some(_) => {
                    polars_bail!(InvalidOperation: "casting to a categorical with rev map is not allowed");
                },
            },
            #[cfg(feature = "dtype-categorical")]
            DataType::Enum(rev_map, ordering) => {
                let Some(rev_map) = rev_map else {
                    polars_bail!(ComputeError: "can not cast / initialize Enum without categories present")
                };
                CategoricalChunked::from_string_to_enum(self, rev_map.get_categories(), *ordering)
                    .map(|ca| {
                        let mut s = ca.into_series();
                        s.rename(self.name().clone());
                        s
                    })
            },
            #[cfg(feature = "dtype-struct")]
            DataType::Struct(fields) => {
                cast_single_to_struct(self.name().clone(), &self.chunks, fields, options)
            },
            #[cfg(feature = "dtype-decimal")]
            DataType::Decimal(precision, scale) => match (precision, scale) {
                (precision, Some(scale)) => {
                    let chunks = self.downcast_iter().map(|arr| {
                        polars_compute::cast::binview_to_decimal(
                            &arr.to_binview(),
                            *precision,
                            *scale,
                        )
                        .to(ArrowDataType::Int128)
                    });
                    Ok(Int128Chunked::from_chunk_iter(self.name().clone(), chunks)
                        .into_decimal_unchecked(*precision, *scale)
                        .into_series())
                },
                (None, None) => self.to_decimal(100),
                _ => {
                    polars_bail!(ComputeError: "expected 'precision' or 'scale' when casting to Decimal")
                },
            },
            #[cfg(feature = "dtype-date")]
            DataType::Date => {
                let result = cast_chunks(&self.chunks, dtype, options)?;
                let out = Series::try_from((self.name().clone(), result))?;
                Ok(out)
            },
            #[cfg(feature = "dtype-datetime")]
            DataType::Datetime(time_unit, time_zone) => match time_zone {
                #[cfg(feature = "timezones")]
                Some(time_zone) => {
                    validate_time_zone(time_zone)?;
                    let result = cast_chunks(
                        &self.chunks,
                        &Datetime(time_unit.to_owned(), Some(time_zone.clone())),
                        options,
                    )?;
                    Series::try_from((self.name().clone(), result))
                },
                _ => {
                    let result =
                        cast_chunks(&self.chunks, &Datetime(time_unit.to_owned(), None), options)?;
                    Series::try_from((self.name().clone(), result))
                },
            },
            _ => cast_impl(self.name().clone(), &self.chunks, dtype, options),
        }
    }

    unsafe fn cast_unchecked(&self, dtype: &DataType) -> PolarsResult<Series> {
        self.cast_with_options(dtype, CastOptions::Overflowing)
    }
}

impl BinaryChunked {
    /// # Safety
    /// String is not validated
    pub unsafe fn to_string_unchecked(&self) -> StringChunked {
        let chunks = self
            .downcast_iter()
            .map(|arr| unsafe { arr.to_utf8view_unchecked() }.boxed())
            .collect();
        let field = Arc::new(Field::new(self.name().clone(), DataType::String));

        let mut ca = StringChunked::new_with_compute_len(field, chunks);

        use StatisticsFlags as F;
        ca.retain_flags_from(self, F::IS_SORTED_ANY | F::CAN_FAST_EXPLODE_LIST);
        ca
    }
}

impl StringChunked {
    pub fn as_binary(&self) -> BinaryChunked {
        let chunks = self
            .downcast_iter()
            .map(|arr| arr.to_binview().boxed())
            .collect();
        let field = Arc::new(Field::new(self.name().clone(), DataType::Binary));

        let mut ca = BinaryChunked::new_with_compute_len(field, chunks);

        use StatisticsFlags as F;
        ca.retain_flags_from(self, F::IS_SORTED_ANY | F::CAN_FAST_EXPLODE_LIST);
        ca
    }
}

impl ChunkCast for BinaryChunked {
    fn cast_with_options(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        match dtype {
            #[cfg(feature = "dtype-struct")]
            DataType::Struct(fields) => {
                cast_single_to_struct(self.name().clone(), &self.chunks, fields, options)
            },
            _ => cast_impl(self.name().clone(), &self.chunks, dtype, options),
        }
    }

    unsafe fn cast_unchecked(&self, dtype: &DataType) -> PolarsResult<Series> {
        match dtype {
            DataType::String => unsafe { Ok(self.to_string_unchecked().into_series()) },
            _ => self.cast_with_options(dtype, CastOptions::Overflowing),
        }
    }
}

impl ChunkCast for BinaryOffsetChunked {
    fn cast_with_options(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        match dtype {
            #[cfg(feature = "dtype-struct")]
            DataType::Struct(fields) => {
                cast_single_to_struct(self.name().clone(), &self.chunks, fields, options)
            },
            _ => cast_impl(self.name().clone(), &self.chunks, dtype, options),
        }
    }

    unsafe fn cast_unchecked(&self, dtype: &DataType) -> PolarsResult<Series> {
        self.cast_with_options(dtype, CastOptions::Overflowing)
    }
}

impl ChunkCast for BooleanChunked {
    fn cast_with_options(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        match dtype {
            #[cfg(feature = "dtype-struct")]
            DataType::Struct(fields) => {
                cast_single_to_struct(self.name().clone(), &self.chunks, fields, options)
            },
            _ => cast_impl(self.name().clone(), &self.chunks, dtype, options),
        }
    }

    unsafe fn cast_unchecked(&self, dtype: &DataType) -> PolarsResult<Series> {
        self.cast_with_options(dtype, CastOptions::Overflowing)
    }
}

/// We cannot cast anything to or from List/LargeList
/// So this implementation casts the inner type
impl ChunkCast for ListChunked {
    fn cast_with_options(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        use DataType::*;
        match dtype {
            List(child_type) => {
                match (self.inner_dtype(), &**child_type) {
                    (old, new) if old == new => Ok(self.clone().into_series()),
                    #[cfg(feature = "dtype-categorical")]
                    (dt, Categorical(None, _) | Enum(_, _))
                        if !matches!(dt, Categorical(_, _) | Enum(_, _) | String | Null) =>
                    {
                        polars_bail!(InvalidOperation: "cannot cast List inner type: '{:?}' to Categorical", dt)
                    },
                    _ => {
                        // ensure the inner logical type bubbles up
                        let (arr, child_type) = cast_list(self, child_type, options)?;
                        // SAFETY: we just cast so the dtype matches.
                        // we must take this path to correct for physical types.
                        unsafe {
                            Ok(Series::from_chunks_and_dtype_unchecked(
                                self.name().clone(),
                                vec![arr],
                                &List(Box::new(child_type)),
                            ))
                        }
                    },
                }
            },
            #[cfg(feature = "dtype-array")]
            Array(child_type, width) => {
                let physical_type = dtype.to_physical();

                // TODO!: properly implement this recursively.
                #[cfg(feature = "dtype-categorical")]
                polars_ensure!(!matches!(&**child_type, Categorical(_, _)), InvalidOperation: "array of categorical is not yet supported");

                // cast to the physical type to avoid logical chunks.
                let chunks = cast_chunks(self.chunks(), &physical_type, options)?;
                // SAFETY: we just cast so the dtype matches.
                // we must take this path to correct for physical types.
                unsafe {
                    Ok(Series::from_chunks_and_dtype_unchecked(
                        self.name().clone(),
                        chunks,
                        &Array(child_type.clone(), *width),
                    ))
                }
            },
            _ => {
                polars_bail!(
                    InvalidOperation: "cannot cast List type (inner: '{:?}', to: '{:?}')",
                    self.inner_dtype(),
                    dtype,
                )
            },
        }
    }

    unsafe fn cast_unchecked(&self, dtype: &DataType) -> PolarsResult<Series> {
        use DataType::*;
        match dtype {
            List(child_type) => cast_list_unchecked(self, child_type),
            _ => self.cast_with_options(dtype, CastOptions::Overflowing),
        }
    }
}

/// We cannot cast anything to or from List/LargeList
/// So this implementation casts the inner type
#[cfg(feature = "dtype-array")]
impl ChunkCast for ArrayChunked {
    fn cast_with_options(&self, dtype: &DataType, options: CastOptions) -> PolarsResult<Series> {
        use DataType::*;
        match dtype {
            Array(child_type, width) => {
                polars_ensure!(
                    *width == self.width(),
                    InvalidOperation: "cannot cast Array to a different width"
                );

                match (self.inner_dtype(), &**child_type) {
                    (old, new) if old == new => Ok(self.clone().into_series()),
                    #[cfg(feature = "dtype-categorical")]
                    (dt, Categorical(None, _) | Enum(_, _)) if !matches!(dt, String) => {
                        polars_bail!(InvalidOperation: "cannot cast Array inner type: '{:?}' to dtype: {:?}", dt, child_type)
                    },
                    _ => {
                        // ensure the inner logical type bubbles up
                        let (arr, child_type) = cast_fixed_size_list(self, child_type, options)?;
                        // SAFETY: we just cast so the dtype matches.
                        // we must take this path to correct for physical types.
                        unsafe {
                            Ok(Series::from_chunks_and_dtype_unchecked(
                                self.name().clone(),
                                vec![arr],
                                &Array(Box::new(child_type), *width),
                            ))
                        }
                    },
                }
            },
            List(child_type) => {
                let physical_type = dtype.to_physical();
                // cast to the physical type to avoid logical chunks.
                let chunks = cast_chunks(self.chunks(), &physical_type, options)?;
                // SAFETY: we just cast so the dtype matches.
                // we must take this path to correct for physical types.
                unsafe {
                    Ok(Series::from_chunks_and_dtype_unchecked(
                        self.name().clone(),
                        chunks,
                        &List(child_type.clone()),
                    ))
                }
            },
            _ => {
                polars_bail!(
                    InvalidOperation: "cannot cast Array type (inner: '{:?}', to: '{:?}')",
                    self.inner_dtype(),
                    dtype,
                )
            },
        }
    }

    unsafe fn cast_unchecked(&self, dtype: &DataType) -> PolarsResult<Series> {
        self.cast_with_options(dtype, CastOptions::Overflowing)
    }
}

// Returns inner data type. This is needed because a cast can instantiate the dtype inner
// values for instance with categoricals
fn cast_list(
    ca: &ListChunked,
    child_type: &DataType,
    options: CastOptions,
) -> PolarsResult<(ArrayRef, DataType)> {
    // We still rechunk because we must bubble up a single data-type
    // TODO!: consider a version that works on chunks and merges the data-types and arrays.
    let ca = ca.rechunk();
    let arr = ca.downcast_as_array();
    // SAFETY: inner dtype is passed correctly
    let s = unsafe {
        Series::from_chunks_and_dtype_unchecked(
            PlSmallStr::EMPTY,
            vec![arr.values().clone()],
            ca.inner_dtype(),
        )
    };
    let new_inner = s.cast_with_options(child_type, options)?;

    let inner_dtype = new_inner.dtype().clone();
    debug_assert_eq!(&inner_dtype, child_type);

    let new_values = new_inner.array_ref(0).clone();

    let dtype = ListArray::<i64>::default_datatype(new_values.dtype().clone());
    let new_arr = ListArray::<i64>::new(
        dtype,
        arr.offsets().clone(),
        new_values,
        arr.validity().cloned(),
    );
    Ok((new_arr.boxed(), inner_dtype))
}

unsafe fn cast_list_unchecked(ca: &ListChunked, child_type: &DataType) -> PolarsResult<Series> {
    // TODO! add chunked, but this must correct for list offsets.
    let ca = ca.rechunk();
    let arr = ca.downcast_as_array();
    // SAFETY: inner dtype is passed correctly
    let s = unsafe {
        Series::from_chunks_and_dtype_unchecked(
            PlSmallStr::EMPTY,
            vec![arr.values().clone()],
            ca.inner_dtype(),
        )
    };
    let new_inner = s.cast_unchecked(child_type)?;
    let new_values = new_inner.array_ref(0).clone();

    let dtype = ListArray::<i64>::default_datatype(new_values.dtype().clone());
    let new_arr = ListArray::<i64>::new(
        dtype,
        arr.offsets().clone(),
        new_values,
        arr.validity().cloned(),
    );
    Ok(ListChunked::from_chunks_and_dtype_unchecked(
        ca.name().clone(),
        vec![Box::new(new_arr)],
        DataType::List(Box::new(child_type.clone())),
    )
    .into_series())
}

// Returns inner data type. This is needed because a cast can instantiate the dtype inner
// values for instance with categoricals
#[cfg(feature = "dtype-array")]
fn cast_fixed_size_list(
    ca: &ArrayChunked,
    child_type: &DataType,
    options: CastOptions,
) -> PolarsResult<(ArrayRef, DataType)> {
    let ca = ca.rechunk();
    let arr = ca.downcast_as_array();
    // SAFETY: inner dtype is passed correctly
    let s = unsafe {
        Series::from_chunks_and_dtype_unchecked(
            PlSmallStr::EMPTY,
            vec![arr.values().clone()],
            ca.inner_dtype(),
        )
    };
    let new_inner = s.cast_with_options(child_type, options)?;

    let inner_dtype = new_inner.dtype().clone();
    debug_assert_eq!(&inner_dtype, child_type);

    let new_values = new_inner.array_ref(0).clone();

    let dtype = FixedSizeListArray::default_datatype(new_values.dtype().clone(), ca.width());
    let new_arr = FixedSizeListArray::new(dtype, ca.len(), new_values, arr.validity().cloned());
    Ok((Box::new(new_arr), inner_dtype))
}

#[cfg(test)]
mod test {
    use crate::chunked_array::cast::CastOptions;
    use crate::prelude::*;

    #[test]
    fn test_cast_list() -> PolarsResult<()> {
        let mut builder = ListPrimitiveChunkedBuilder::<Int32Type>::new(
            PlSmallStr::from_static("a"),
            10,
            10,
            DataType::Int32,
        );
        builder.append_opt_slice(Some(&[1i32, 2, 3]));
        builder.append_opt_slice(Some(&[1i32, 2, 3]));
        let ca = builder.finish();

        let new = ca.cast_with_options(
            &DataType::List(DataType::Float64.into()),
            CastOptions::Strict,
        )?;

        assert_eq!(new.dtype(), &DataType::List(DataType::Float64.into()));
        Ok(())
    }

    #[test]
    #[cfg(feature = "dtype-categorical")]
    fn test_cast_noop() {
        // check if we can cast categorical twice without panic
        let ca = StringChunked::new(PlSmallStr::from_static("foo"), &["bar", "ham"]);
        let out = ca
            .cast_with_options(
                &DataType::Categorical(None, Default::default()),
                CastOptions::Strict,
            )
            .unwrap();
        let out = out
            .cast(&DataType::Categorical(None, Default::default()))
            .unwrap();
        assert!(matches!(out.dtype(), &DataType::Categorical(_, _)))
    }
}
