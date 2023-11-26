//! Table row interface.
use std::borrow::Cow;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::{
    chunked_array::builder::{BinaryChunkedBuilderCow, Utf8ChunkedBuilderCow},
    prelude::*,
};

pub trait TableRow: Sized {
    /// The frame struct for this row type.
    type Frame<'a>: FrameStruct<'a, Self>;
    /// The frame builder type for this row type.
    type Builder: FrameBuilder<Self>;

    /// Get the schema for this table row.
    fn schema() -> Schema;
}

/// Interface for data frame structs for deserialization.
///
/// Frame structs store references to the data frame's columns so we only need
/// to extract them from the frame once.
pub trait FrameStruct<'a, R>
where
    R: TableRow + Sized,
    Self: Sized,
{
    fn new(df: &'a DataFrame) -> PolarsResult<Self>;
    fn read_row(&mut self, idx: usize) -> PolarsResult<R>;
}

/// Interface for data frame builders.
pub trait FrameBuilder<R>
where
    R: TableRow + Sized,
{
    /// Instantiate a frame builder with a specified capacity.
    fn with_capacity(cap: usize) -> Self;
    /// Add a row to the frame builder.
    fn append_row(&mut self, row: R);
    /// Finish the builder and create a data frame.
    fn build(self) -> PolarsResult<DataFrame>;

    /// Add an iterable of items to the frame.
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = R>,
    {
        for row in iter {
            self.append_row(row);
        }
    }
}

/// Trait for column types.
pub trait ColType: Sized {
    type PolarsType;
    type Array;
    type Builder;

    /// Create a new builder.
    fn column_builder(name: &str, cap: usize) -> Self::Builder;

    /// Append this item to a builder.
    fn append_to_column(self, b: &mut Self::Builder);

    /// Cast a series to the appropriate chunked type.
    fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array>;

    /// Read a value from an array.
    fn read_from_column(a: &Self::Array, pos: usize) -> PolarsResult<Self>;
}

macro_rules! col_type {
    ($rs:ident, $pl:ty) => {
        col_type!($rs, $pl, ChunkedArray<$pl>, PrimitiveChunkedBuilder<$pl>);
    };
    ($rs:ident, $pl:ty, $a:ty, $bld: ty) => {
        col_type!($rs, $pl, $a, $bld, $rs);
    };
    ($rs:ty, $pl:ty, $a:ty, $bld: ty, $cast:ident) => {
        impl ColType for $rs {
            type PolarsType = $pl;
            type Array = $a;
            type Builder = $bld;

            fn column_builder(name: &str, cap: usize) -> Self::Builder {
                Self::Builder::new(name, cap)
            }

            fn append_to_column(self, b: &mut Self::Builder) {
                b.append_value(self.into());
            }

            fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
                s.$cast()
            }

            fn read_from_column(a: &Self::Array, pos: usize) -> PolarsResult<Self> {
                a.get(pos)
                    .ok_or_else(|| PolarsError::NoData("required column value is null".into()))
                    .map(|x| x.into())
            }
        }
        // just manually derive the option, bounds are being a pain
        impl ColType for Option<$rs> {
            type PolarsType = $pl;
            type Array = $a;
            type Builder = $bld;

            fn column_builder(name: &str, cap: usize) -> Self::Builder {
                Self::Builder::new(name, cap)
            }

            fn append_to_column(self, b: &mut Self::Builder) {
                b.append_option(self.map(Into::into));
            }

            fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
                s.$cast()
            }

            fn read_from_column(a: &Self::Array, pos: usize) -> PolarsResult<Self> {
                Ok(a.get(pos).map(|x| x.into()))
            }
        }
    };
}

col_type!(bool, BooleanType, BooleanChunked, BooleanChunkedBuilder);
col_type!(i8, Int8Type);
col_type!(i16, Int16Type);
col_type!(i32, Int32Type);
col_type!(i64, Int64Type);
col_type!(u8, UInt8Type);
col_type!(u16, UInt16Type);
col_type!(u32, UInt32Type);
col_type!(u64, UInt64Type);
col_type!(f32, Float32Type);
col_type!(f64, Float64Type);
// col_type!(&str, Utf8Type, Utf8Chunked, Utf8ChunkedBuilderCow, utf8);
col_type!(String, Utf8Type, Utf8Chunked, Utf8ChunkedBuilderCow, utf8);

// It would be nice to shrink this, but Polars doesn't expose the expected types
// — its date handling only supports operating on chunks, not individual values.
// We use the same logic to convert a date to Parquet's standard “days since the
// epoch” format.
fn convert_naive_date(date: NaiveDate) -> i32 {
    let dt = NaiveDateTime::new(date, NaiveTime::default());
    (dt.timestamp() / (24 * 60 * 60)) as i32
}

fn convert_to_naive_date(ts: i32) -> PolarsResult<NaiveDate> {
    let ts = (ts as i64) * 24 * 60 * 60;
    let dt = NaiveDateTime::from_timestamp_millis(ts * 1000);
    dt.ok_or_else(|| PolarsError::NoData("invalid date".into()))
        .map(|dt| dt.date())
}

impl ColType for NaiveDate {
    type PolarsType = NaiveDate;
    type Array = DateChunked;
    type Builder = PrimitiveChunkedBuilder<Int32Type>;

    fn column_builder(name: &str, cap: usize) -> Self::Builder {
        Self::Builder::new(name, cap)
    }

    fn append_to_column(self, b: &mut Self::Builder) {
        b.append_value(convert_naive_date(self));
    }

    fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
        s.date()
    }

    fn read_from_column(a: &Self::Array, pos: usize) -> PolarsResult<Self> {
        let res = a.get(pos).map(convert_to_naive_date).transpose()?;
        res.ok_or_else(|| PolarsError::NoData("required column value is null".into()))
    }
}

// just manually derive the option, bounds are being a pain
impl ColType for Option<NaiveDate> {
    type PolarsType = NaiveDate;
    type Array = DateChunked;
    type Builder = PrimitiveChunkedBuilder<Int32Type>;

    fn column_builder(name: &str, cap: usize) -> Self::Builder {
        Self::Builder::new(name, cap)
    }

    fn append_to_column(self, b: &mut Self::Builder) {
        b.append_option(self.map(convert_naive_date));
    }

    fn cast_series<'a>(s: &'a Series) -> PolarsResult<&'a Self::Array> {
        s.date()
    }

    fn read_from_column(a: &Self::Array, pos: usize) -> PolarsResult<Self> {
        a.get(pos).map(convert_to_naive_date).transpose()
    }
}
