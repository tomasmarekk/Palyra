//! Bounded image-container inspection and metadata stripping.

use std::collections::BTreeSet;

use crate::gateway::IMAGE_OBSERVE_TOOL_NAME;

pub(super) fn sanitize_png(bytes: &[u8]) -> Result<(Vec<u8>, Vec<String>, bool), String> {
    if !bytes.starts_with(b"\x89PNG\r\n\x1A\n") {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed PNG signature"));
    }
    let mut output = bytes[..8].to_vec();
    let mut cursor = 8usize;
    let mut stripped = BTreeSet::new();
    let mut saw_ihdr = false;
    let mut saw_idat = false;
    let mut saw_iend = false;
    let mut has_alpha = false;

    while cursor < bytes.len() {
        if cursor.saturating_add(12) > bytes.len() {
            return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed PNG chunk"));
        }
        let data_len =
            u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap_or_default()) as usize;
        let chunk_end = cursor
            .checked_add(12)
            .and_then(|value| value.checked_add(data_len))
            .ok_or_else(|| format!("{IMAGE_OBSERVE_TOOL_NAME} malformed PNG chunk length"))?;
        if chunk_end > bytes.len() {
            return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed PNG chunk length"));
        }
        let chunk_type = &bytes[cursor + 4..cursor + 8];
        let data = &bytes[cursor + 8..cursor + 8 + data_len];
        match chunk_type {
            b"IHDR" => {
                if data.len() != 13 {
                    return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed PNG IHDR"));
                }
                saw_ihdr = true;
                has_alpha = matches!(data[9], 4 | 6);
            }
            b"IDAT" => saw_idat = true,
            b"IEND" => saw_iend = true,
            b"tRNS" => has_alpha = true,
            _ => {}
        }
        let critical = chunk_type.first().is_some_and(u8::is_ascii_uppercase);
        let preserve = critical || chunk_type == b"tRNS";
        if preserve {
            output.extend_from_slice(&bytes[cursor..chunk_end]);
        } else {
            stripped.insert(png_metadata_kind(chunk_type));
        }
        cursor = chunk_end;
        if chunk_type == b"IEND" {
            break;
        }
    }
    if !saw_ihdr || !saw_idat || !saw_iend {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} PNG is missing required chunks"));
    }
    if cursor != bytes.len() {
        stripped.insert("png_trailing_data".to_owned());
    }
    Ok((output, stripped.into_iter().collect(), has_alpha))
}

fn png_metadata_kind(chunk_type: &[u8]) -> String {
    match chunk_type {
        b"eXIf" => "exif".to_owned(),
        b"iCCP" => "color_profile".to_owned(),
        b"iTXt" | b"tEXt" | b"zTXt" => "text_metadata".to_owned(),
        _ => "png_ancillary_metadata".to_owned(),
    }
}

pub(super) fn sanitize_jpeg(bytes: &[u8]) -> Result<(Vec<u8>, Vec<String>, bool), String> {
    if !bytes.starts_with(b"\xFF\xD8") {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed JPEG signature"));
    }
    let mut output = bytes[..2].to_vec();
    let mut cursor = 2usize;
    let mut stripped = BTreeSet::new();
    let mut copied_scan = false;
    while cursor < bytes.len() {
        let marker_start = cursor;
        if bytes[cursor] != 0xFF {
            return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed JPEG marker"));
        }
        while cursor < bytes.len() && bytes[cursor] == 0xFF {
            cursor = cursor.saturating_add(1);
        }
        let marker = *bytes
            .get(cursor)
            .ok_or_else(|| format!("{IMAGE_OBSERVE_TOOL_NAME} malformed JPEG marker"))?;
        cursor = cursor.saturating_add(1);
        if marker == 0xDA {
            output.extend_from_slice(&bytes[marker_start..]);
            copied_scan = true;
            break;
        }
        if marker == 0xD9 {
            output.extend_from_slice(&bytes[marker_start..cursor]);
            copied_scan = true;
            break;
        }
        if matches!(marker, 0x01 | 0xD0..=0xD7) {
            output.extend_from_slice(&bytes[marker_start..cursor]);
            continue;
        }
        let length_end = cursor.saturating_add(2);
        let segment_length = u16::from_be_bytes(
            bytes
                .get(cursor..length_end)
                .ok_or_else(|| format!("{IMAGE_OBSERVE_TOOL_NAME} malformed JPEG segment"))?
                .try_into()
                .unwrap_or_default(),
        ) as usize;
        if segment_length < 2 {
            return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed JPEG segment"));
        }
        let segment_end = cursor
            .checked_add(segment_length)
            .ok_or_else(|| format!("{IMAGE_OBSERVE_TOOL_NAME} malformed JPEG segment length"))?;
        if segment_end > bytes.len() {
            return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed JPEG segment length"));
        }
        if matches!(marker, 0xE1..=0xEF | 0xFE) {
            stripped.insert(match marker {
                0xE1 => "exif_or_xmp".to_owned(),
                0xE2 => "color_profile".to_owned(),
                0xFE => "jpeg_comment".to_owned(),
                _ => "jpeg_application_metadata".to_owned(),
            });
        } else {
            output.extend_from_slice(&bytes[marker_start..segment_end]);
        }
        cursor = segment_end;
    }
    if !copied_scan {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} JPEG has no image scan"));
    }
    Ok((output, stripped.into_iter().collect(), false))
}

pub(super) fn sanitize_webp(bytes: &[u8]) -> Result<(Vec<u8>, Vec<String>, bool), String> {
    if bytes.len() < 12 || !bytes.starts_with(b"RIFF") || bytes.get(8..12) != Some(b"WEBP") {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed WebP signature"));
    }
    let mut output = b"RIFF\0\0\0\0WEBP".to_vec();
    let mut cursor = 12usize;
    let mut stripped = BTreeSet::new();
    let mut saw_pixels = false;
    let mut has_alpha = false;
    while cursor < bytes.len() {
        if cursor.saturating_add(8) > bytes.len() {
            return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed WebP chunk"));
        }
        let chunk_type = &bytes[cursor..cursor + 4];
        let data_len =
            u32::from_le_bytes(bytes[cursor + 4..cursor + 8].try_into().unwrap_or_default())
                as usize;
        let padded_len = data_len.saturating_add(data_len % 2);
        let chunk_end = cursor
            .checked_add(8)
            .and_then(|value| value.checked_add(padded_len))
            .ok_or_else(|| format!("{IMAGE_OBSERVE_TOOL_NAME} malformed WebP chunk length"))?;
        if chunk_end > bytes.len() {
            return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} malformed WebP chunk length"));
        }
        if matches!(chunk_type, b"EXIF" | b"XMP " | b"ICCP") {
            stripped.insert(
                match chunk_type {
                    b"EXIF" => "exif",
                    b"XMP " => "xmp",
                    _ => "color_profile",
                }
                .to_owned(),
            );
        } else {
            output.extend_from_slice(&bytes[cursor..chunk_end]);
        }
        if matches!(chunk_type, b"VP8 " | b"VP8L") {
            saw_pixels = true;
        }
        if chunk_type == b"ALPH"
            || chunk_type == b"VP8X" && bytes.get(cursor + 8).is_some_and(|flags| flags & 0x10 != 0)
            || chunk_type == b"VP8L"
        {
            has_alpha = true;
        }
        cursor = chunk_end;
    }
    if !saw_pixels {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} WebP has no image payload"));
    }
    let riff_size = u32::try_from(output.len().saturating_sub(8))
        .map_err(|_| format!("{IMAGE_OBSERVE_TOOL_NAME} sanitized WebP is too large"))?;
    output[4..8].copy_from_slice(&riff_size.to_le_bytes());
    Ok((output, stripped.into_iter().collect(), has_alpha))
}

pub(super) fn image_mime_type(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"\x89PNG\r\n\x1A\n") {
        "image/png"
    } else if bytes.starts_with(b"\xFF\xD8\xFF") {
        "image/jpeg"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else {
        "application/octet-stream"
    }
}

pub(super) fn image_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    png_dimensions(bytes)
        .or_else(|| jpeg_dimensions(bytes))
        .or_else(|| webp_dimensions(bytes))
        .or_else(|| gif_dimensions(bytes))
}

fn png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 24 || !bytes.starts_with(b"\x89PNG\r\n\x1A\n") {
        return None;
    }
    let width = u32::from_be_bytes(bytes.get(16..20)?.try_into().ok()?);
    let height = u32::from_be_bytes(bytes.get(20..24)?.try_into().ok()?);
    Some((width, height))
}

fn gif_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 10 || !(bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) {
        return None;
    }
    let width = u16::from_le_bytes(bytes.get(6..8)?.try_into().ok()?);
    let height = u16::from_le_bytes(bytes.get(8..10)?.try_into().ok()?);
    Some((u32::from(width), u32::from(height)))
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 4 || !bytes.starts_with(b"\xFF\xD8") {
        return None;
    }
    let mut index = 2usize;
    while index + 9 < bytes.len() {
        if bytes[index] != 0xFF {
            index = index.saturating_add(1);
            continue;
        }
        while index < bytes.len() && bytes[index] == 0xFF {
            index = index.saturating_add(1);
        }
        let marker = *bytes.get(index)?;
        index = index.saturating_add(1);
        if marker == 0xD9 || marker == 0xDA {
            return None;
        }
        let length = u16::from_be_bytes(bytes.get(index..index + 2)?.try_into().ok()?) as usize;
        if length < 2 || index + length > bytes.len() {
            return None;
        }
        if matches!(
            marker,
            0xC0 | 0xC1
                | 0xC2
                | 0xC3
                | 0xC5
                | 0xC6
                | 0xC7
                | 0xC9
                | 0xCA
                | 0xCB
                | 0xCD
                | 0xCE
                | 0xCF
        ) {
            let height = u16::from_be_bytes(bytes.get(index + 3..index + 5)?.try_into().ok()?);
            let width = u16::from_be_bytes(bytes.get(index + 5..index + 7)?.try_into().ok()?);
            return Some((u32::from(width), u32::from(height)));
        }
        index = index.saturating_add(length);
    }
    None
}

fn webp_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 30 || !bytes.starts_with(b"RIFF") || bytes.get(8..12) != Some(b"WEBP") {
        return None;
    }
    let chunk_type = bytes.get(12..16)?;
    match chunk_type {
        b"VP8X" => Some((
            1 + little_endian_u24(bytes.get(24..27)?),
            1 + little_endian_u24(bytes.get(27..30)?),
        )),
        b"VP8L" if bytes.get(20) == Some(&0x2F) => {
            let packed = u32::from_le_bytes([
                *bytes.get(21)?,
                *bytes.get(22)?,
                *bytes.get(23)?,
                *bytes.get(24)?,
            ]);
            Some(((packed & 0x3FFF) + 1, ((packed >> 14) & 0x3FFF) + 1))
        }
        b"VP8 " if bytes.get(23..26) == Some(b"\x9D\x01\x2A") => {
            let width = u16::from_le_bytes(bytes.get(26..28)?.try_into().ok()?) & 0x3FFF;
            let height = u16::from_le_bytes(bytes.get(28..30)?.try_into().ok()?) & 0x3FFF;
            Some((u32::from(width), u32::from(height)))
        }
        _ => None,
    }
}

fn little_endian_u24(bytes: &[u8]) -> u32 {
    u32::from(bytes[0]) | u32::from(bytes[1]) << 8 | u32::from(bytes[2]) << 16
}
