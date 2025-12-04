//! Low-level helpers for `RowBinary` encoding.

use std::io::{self, Read, Write};

use crate::error::{Error, Result};

/// Writes an unsigned varint using `ClickHouse`'s encoding.
///
/// # Errors
///
/// Returns [`std::io::Error`] through [`crate::error::Error`] when the backing
/// writer fails.
pub fn write_uvarint<W: Write + ?Sized>(mut value: u64, writer: &mut W) -> Result<()> {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            break;
        }
    }
    Ok(())
}

/// Reads an unsigned varint from the stream. Returns `Ok(None)` on EOF.
///
/// # Errors
///
/// Returns [`std::io::Error`] through [`crate::error::Error`] when the backing
/// reader fails or the stream ends unexpectedly.
pub fn read_uvarint<R: Read + ?Sized>(reader: &mut R) -> Result<Option<u64>> {
    let mut shift = 0;
    let mut value = 0_u64;
    let mut first = true;
    loop {
        let mut byte = [0_u8; 1];
        match reader.read(&mut byte)? {
            0 => {
                if first {
                    return Ok(None);
                }
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF while reading varint",
                )));
            }
            1 => {
                first = false;
                let chunk = u64::from(byte[0] & 0x7F);
                value |= chunk << shift;
                if byte[0] & 0x80 == 0 {
                    return Ok(Some(value));
                }
                shift += 7;
                if shift >= 64 {
                    return Err(Error::InvalidValue("varint exceeds 64 bits"));
                }
            }
            _ => {
                return Err(Error::Internal(
                    "unexpected read size while decoding varint",
                ));
            }
        }
    }
}

/// Writes a length-prefixed byte slice (used by `RowBinary` for strings).
///
/// # Errors
///
/// Returns [`std::io::Error`] through [`crate::error::Error`] when the backing
/// writer fails.
pub fn write_bytes<W: Write + ?Sized>(value: &[u8], writer: &mut W) -> Result<()> {
    write_uvarint(value.len() as u64, writer)?;
    writer.write_all(value)?;
    Ok(())
}

/// Writes a UTF-8 string with length prefix.
///
/// # Errors
///
/// Returns [`std::io::Error`] through [`crate::error::Error`] when the backing
/// writer fails.
pub fn write_string<W: Write + ?Sized>(value: &str, writer: &mut W) -> Result<()> {
    write_bytes(value.as_bytes(), writer)
}

/// Reads a length-prefixed byte slice.
///
/// Returns `Ok(None)` on EOF when no length could be read.
///
/// # Errors
///
/// Returns [`std::io::Error`] through [`crate::error::Error`] when the backing
/// reader fails or the data is malformed.
pub fn read_bytes<R: Read + ?Sized>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    let Some(len) = read_uvarint(reader)? else {
        return Ok(None);
    };
    let len = usize::try_from(len).map_err(|_| Error::Overflow("byte length too large"))?;
    let mut buf = vec![0_u8; len];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}

/// Reads a length-prefixed UTF-8 string.
///
/// Returns `Ok(None)` on EOF when no length could be read.
///
/// # Errors
///
/// Returns [`std::io::Error`] through [`crate::error::Error`] when the backing
/// reader fails or the data is not valid UTF-8.
pub fn read_string<R: Read + ?Sized>(reader: &mut R) -> Result<Option<String>> {
    let Some(bytes) = read_bytes(reader)? else {
        return Ok(None);
    };
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| Error::InvalidValue("invalid UTF-8 string"))
}
