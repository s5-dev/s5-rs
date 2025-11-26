use core::fmt;
use minicbor::CborLen;
use minicbor::data::{Int, Tag, Type};
use minicbor::decode::{Decode, Error};
use minicbor::encode::{self, Encode, Encoder, Write};

/// Maximum size for bytes/string values to prevent memory exhaustion from
/// untrusted CBOR input. 16 MiB is generous for typical use cases.
const MAX_BYTES_STRING_LEN: usize = 16 * 1024 * 1024;

/// Representation of possible CBOR values.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    Int(Int),
    F16(f32),
    F32(f32),
    F64(f64),
    Bytes(bytes::Bytes),
    String(String),
    Array(u64),
    Map(u64),
    Tag(Tag),
    Simple(u8),
    Break,
    Null,
    Undefined,
    /// Start of indefinite byte string.
    BeginBytes,
    /// Start of indefinite text string.
    BeginString,
    /// Start of indefinite array.
    BeginArray,
    /// Start of indefinite map.
    BeginMap,
}

/// Pretty print a token.
///
/// Since we only show a single token we can not use diagnostic notation
/// as in the `Display` impl of [`crate::decode::Tokenizer`]. Instead, the following
/// syntax is used:
///
/// - Numeric values and booleans are displayed as in Rust. Floats are always
///   shown in scientific notation.
/// - Text strings are displayed in double quotes.
/// - Byte strings are displayed in single quotes prefixed with `h` and
///   hex-encoded, e.g. `h'01 02 ef'`.
/// - An array is displayed as `A[n]` where `n` denotes the number of elements.
///   The following `n` tokens are elements of this array.
/// - A map is displayed as `M[n]` where `n` denotes the number of pairs.
///   The following `n` tokens are entries of this map.
/// - Tags are displayed with `T(t)` where `t` is the tag number.
/// - Simple values are displayed as `simple(n)` where `n` denotes the numeric
///   value.
/// - Indefinite items start with:
///
///     * `?B[` for byte strings,
///     * `?S[` for text strings,
///     * `?A[` for arrays,
///     * `?M[` for maps,
///
///   and end with `]` when a `Value::Break` is encountered. All tokens
///   in between belong to the indefinite container.
/// - `Value::Null` is displayed as `null` and `Value::Undefined` as `undefined`.
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Bool(b) => write!(f, "{}", b),
            Value::U8(n) => write!(f, "{}", n),
            Value::U16(n) => write!(f, "{}", n),
            Value::U32(n) => write!(f, "{}", n),
            Value::U64(n) => write!(f, "{}", n),
            Value::I8(n) => write!(f, "{}", n),
            Value::I16(n) => write!(f, "{}", n),
            Value::I32(n) => write!(f, "{}", n),
            Value::I64(n) => write!(f, "{}", n),
            Value::Int(n) => write!(f, "{}", n),
            Value::F16(n) => write!(f, "{:e}", n),
            Value::F32(n) => write!(f, "{:e}", n),
            Value::F64(n) => write!(f, "{:e}", n),
            Value::String(n) => write!(f, "\"{}\"", n),
            Value::Array(n) => write!(f, "A[{}]", n),
            Value::Map(n) => write!(f, "M[{}]", n),
            Value::Tag(t) => write!(f, "T({})", u64::from(t)),
            Value::Simple(n) => write!(f, "simple({})", n),
            Value::Break => f.write_str("]"),
            Value::Null => f.write_str("null"),
            Value::Undefined => f.write_str("undefined"),
            Value::BeginBytes => f.write_str("?B["),
            Value::BeginString => f.write_str("?S["),
            Value::BeginArray => f.write_str("?A["),
            Value::BeginMap => f.write_str("?M["),
            Value::Bytes(b) => {
                f.write_str("h'")?;
                let mut i = b.len();
                for x in b {
                    if i > 1 {
                        write!(f, "{:02x} ", x)?
                    } else {
                        write!(f, "{:02x}", x)?
                    }
                    i -= 1;
                }
                f.write_str("'")
            }
        }
    }
}

impl<'b, C> Decode<'b, C> for Value {
    fn decode(d: &mut minicbor::Decoder<'b>, _: &mut C) -> Result<Self, Error> {
        match d.datatype()? {
            Type::Bool => d.bool().map(Value::Bool),
            Type::U8 => d.u8().map(Value::U8),
            Type::U16 => d.u16().map(Value::U16),
            Type::U32 => d.u32().map(Value::U32),
            Type::U64 => d.u64().map(Value::U64),
            Type::I8 => d.i8().map(Value::I8),
            Type::I16 => d.i16().map(Value::I16),
            Type::I32 => d.i32().map(Value::I32),
            Type::I64 => d.i64().map(Value::I64),
            Type::Int => d.int().map(Value::Int),
            Type::F16 => d.f16().map(Value::F16),
            Type::F32 => d.f32().map(Value::F32),
            Type::F64 => d.f64().map(Value::F64),
            Type::Bytes => {
                let p = d.position();
                let val = d.bytes()?;
                if val.len() > MAX_BYTES_STRING_LEN {
                    return Err(Error::message("bytes value exceeds size limit").at(p));
                }
                Ok(Value::Bytes(bytes::Bytes::copy_from_slice(val)))
            }
            Type::String => {
                let p = d.position();
                let val = d.str()?;
                if val.len() > MAX_BYTES_STRING_LEN {
                    return Err(Error::message("string value exceeds size limit").at(p));
                }
                Ok(Value::String(val.into()))
            }
            Type::Tag => d.tag().map(Value::Tag),
            Type::Simple => d.simple().map(Value::Simple),
            Type::Array => {
                let p = d.position();
                if let Some(n) = d.array()? {
                    Ok(Value::Array(n))
                } else {
                    Err(Error::type_mismatch(Type::Array)
                        .at(p)
                        .with_message("missing array length"))
                }
            }
            Type::Map => {
                let p = d.position();
                if let Some(n) = d.map()? {
                    Ok(Value::Map(n))
                } else {
                    Err(Error::type_mismatch(Type::Map)
                        .at(p)
                        .with_message("missing map length"))
                }
            }
            Type::BytesIndef => {
                skip_byte(d);
                Ok(Value::BeginBytes)
            }
            Type::StringIndef => {
                skip_byte(d);
                Ok(Value::BeginString)
            }
            Type::ArrayIndef => {
                skip_byte(d);
                Ok(Value::BeginArray)
            }
            Type::MapIndef => {
                skip_byte(d);
                Ok(Value::BeginMap)
            }
            Type::Null => {
                skip_byte(d);
                Ok(Value::Null)
            }
            Type::Undefined => {
                skip_byte(d);
                Ok(Value::Undefined)
            }
            Type::Break => {
                skip_byte(d);
                Ok(Value::Break)
            }
            t @ Type::Unknown(_) => Err(Error::type_mismatch(t)
                .at(d.position())
                .with_message("unknown cbor type")),
        }
    }
}

fn skip_byte(d: &mut minicbor::Decoder<'_>) {
    d.set_position(d.position() + 1)
}

impl<C> Encode<C> for Value {
    fn encode<W: Write>(
        &self,
        e: &mut Encoder<W>,
        _: &mut C,
    ) -> Result<(), encode::Error<W::Error>> {
        match self {
            Value::Bool(val) => e.bool(*val)?,
            Value::U8(val) => e.u8(*val)?,
            Value::U16(val) => e.u16(*val)?,
            Value::U32(val) => e.u32(*val)?,
            Value::U64(val) => e.u64(*val)?,
            Value::I8(val) => e.i8(*val)?,
            Value::I16(val) => e.i16(*val)?,
            Value::I32(val) => e.i32(*val)?,
            Value::I64(val) => e.i64(*val)?,
            Value::Int(val) => e.int(*val)?,
            Value::F16(val) => e.f16(*val)?,
            Value::F32(val) => e.f32(*val)?,
            Value::F64(val) => e.f64(*val)?,
            Value::Bytes(val) => e.bytes(val.as_ref())?,
            Value::String(val) => e.str(val.as_str())?,
            Value::Array(val) => e.array(*val)?,
            Value::Map(val) => e.map(*val)?,
            Value::Tag(val) => e.tag(*val)?,
            Value::Simple(val) => e.simple(*val)?,
            Value::Break => e.end()?,
            Value::Null => e.null()?,
            Value::Undefined => e.undefined()?,
            Value::BeginBytes => e.begin_bytes()?,
            Value::BeginString => e.begin_str()?,
            Value::BeginArray => e.begin_array()?,
            Value::BeginMap => e.begin_map()?,
        };
        Ok(())
    }
}

impl<C> CborLen<C> for Value {
    fn cbor_len(&self, ctx: &mut C) -> usize {
        match self {
            Value::Bool(val) => val.cbor_len(ctx),
            Value::U8(val) => val.cbor_len(ctx),
            Value::U16(val) => val.cbor_len(ctx),
            Value::U32(val) => val.cbor_len(ctx),
            Value::U64(val) => val.cbor_len(ctx),
            Value::I8(val) => val.cbor_len(ctx),
            Value::I16(val) => val.cbor_len(ctx),
            Value::I32(val) => val.cbor_len(ctx),
            Value::I64(val) => val.cbor_len(ctx),
            Value::Int(val) => val.cbor_len(ctx),
            Value::F16(val) => val.cbor_len(ctx),
            Value::F32(val) => val.cbor_len(ctx),
            Value::F64(val) => val.cbor_len(ctx),
            Value::Bytes(val) => val.cbor_len(ctx),
            Value::String(val) => val.cbor_len(ctx),
            Value::Array(val) => val.cbor_len(ctx),
            Value::Map(val) => val.cbor_len(ctx),
            Value::Tag(val) => val.cbor_len(ctx),
            Value::Simple(val) => val.cbor_len(ctx),
            Value::Break => 1,
            Value::Null => 1,
            Value::Undefined => 1,
            Value::BeginBytes => 1,
            Value::BeginString => 1,
            Value::BeginArray => 1,
            Value::BeginMap => 1,
        }
    }
}
