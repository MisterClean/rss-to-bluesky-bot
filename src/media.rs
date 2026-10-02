//! Sequential, bounded source-image preparation and durable exact-byte caching.
//!
//! JPEG and PNG originals that already satisfy the output constraints are kept
//! byte-for-byte. GIF and WebP are rendered as their first static frame.

use crate::{Error, Result, config::MediaConfig, model::Article};
use image::{
    DynamicImage, ImageDecoder, ImageEncoder, ImageFormat, ImageReader, Limits,
    codecs::{
        jpeg::JpegEncoder,
        png::{CompressionType, FilterType, PngEncoder},
    },
    metadata::Orientation,
};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    borrow::Cow,
    fs,
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use url::Url;

/// Encoded image with dimensions in display orientation.
pub struct PreparedImage {
    pub bytes: Vec<u8>,
    pub mime: String,
    pub width: u32,
    pub height: u32,
}

/// A frozen upload, stored beside its exact bytes for same-record recovery.
#[derive(Serialize, Deserialize)]
pub struct CachedImage {
    pub mime: String,
    pub image: Value,
}

/// Prefer article originals and largest responsive images, keeping feed fallbacks.
pub async fn discover(
    client: &reqwest::Client,
    article: &Article,
    settings: &MediaConfig,
    http: &crate::config::HttpConfig,
) -> Result<Vec<String>> {
    let mut feed_images = Vec::new();
    for source in &article.image_urls {
        add_candidate(&mut feed_images, source, &article.url);
    }
    if !settings.discover_from_article {
        return Ok(feed_images);
    }
    let bytes = crate::http::get_bytes_with_timeout(
        client,
        &article.url,
        http.max_article_bytes,
        Duration::from_secs(http.timeout_seconds),
    )
    .await;
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(_) if !feed_images.is_empty() => return Ok(feed_images),
        Err(error) => return Err(error),
    };
    let html = Html::parse_document(&String::from_utf8_lossy(&bytes));
    let mut candidates = Vec::new();
    for selector in [
        "meta[property='og:image'], meta[property='og:image:url'], meta[property='og:image:secure_url']",
        "meta[name='twitter:image'], meta[property='twitter:image'], meta[name='twitter:image:src']",
    ] {
        let selector = Selector::parse(selector)
            .map_err(|_| Error::Media("image selector is invalid".into()))?;
        for element in html.select(&selector).take(24) {
            if candidates.len() >= 24 {
                break;
            }
            if let Some(source) = element.value().attr("content") {
                add_candidate(&mut candidates, source, &article.url);
            }
        }
    }
    let images =
        Selector::parse("article img, main img, article picture source, main picture source")
            .map_err(|_| Error::Media("image selector is invalid".into()))?;
    let mut responsive = Vec::new();
    let mut fallback = Vec::new();
    for element in html.select(&images).take(64) {
        let attributes = element.value();
        let displayed_width = attributes
            .attr("width")
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(1.0);
        let largest = ["data-srcset", "data-lazy-srcset", "srcset"]
            .into_iter()
            .filter_map(|name| attributes.attr(name))
            .filter_map(|value| largest_srcset(value, displayed_width))
            .max_by(|(_, left), (_, right)| left.total_cmp(right));
        if let Some((source, size)) = largest {
            responsive.push((source, size));
        }
        for name in [
            "data-original",
            "data-original-src",
            "data-lazy-src",
            "data-src",
            "src",
        ] {
            if let Some(source) = attributes.attr(name) {
                fallback.push((source, displayed_width));
            }
        }
    }
    responsive.sort_by(|(_, left), (_, right)| right.total_cmp(left));
    fallback.sort_by(|(_, left), (_, right)| right.total_cmp(left));
    for (source, _) in responsive.into_iter().chain(fallback) {
        // Reserve room for known feed images if all article candidates fail.
        if candidates.len() >= 24 {
            break;
        }
        add_candidate(&mut candidates, source, &article.url);
    }
    for source in &feed_images {
        add_candidate(&mut candidates, source, &article.url);
    }
    Ok(candidates)
}

fn largest_srcset(srcset: &str, displayed_width: f64) -> Option<(&str, f64)> {
    srcset
        .split(',')
        .filter_map(|candidate| {
            let mut parts = candidate.split_whitespace();
            let source = parts.next()?;
            let size = match parts.next() {
                Some(descriptor) => {
                    let (number, scale) = if let Some(width) = descriptor.strip_suffix('w') {
                        (width, 1.0)
                    } else if let Some(density) = descriptor.strip_suffix('x') {
                        (density, displayed_width)
                    } else {
                        return None;
                    };
                    number
                        .parse::<f64>()
                        .ok()
                        .filter(|value| value.is_finite() && *value > 0.0)?
                        * scale
                }
                None => displayed_width,
            };
            Some((source, size))
        })
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
}

fn add_candidate(candidates: &mut Vec<String>, source: &str, base: &str) {
    // Cap URL metadata independently of the number of selected uploads.
    if candidates.len() >= 32 || source.len() > 8192 {
        return;
    }
    let Ok(base) = Url::parse(base) else {
        return;
    };
    let Ok(url) = base.join(source) else {
        return;
    };
    if matches!(url.scheme(), "http" | "https")
        && url.username().is_empty()
        && url.password().is_none()
    {
        let value = url.to_string();
        if !candidates.contains(&value) {
            candidates.push(value);
        }
    }
}

/// Prepare an image using the same bounded pipeline as live publishing.
pub fn process_image(bytes: &[u8], settings: &MediaConfig) -> Result<PreparedImage> {
    if bytes.len() > settings.max_download_bytes {
        return Err(Error::Media("source exceeds download budget".into()));
    }
    process(bytes.to_vec(), settings)
}

/// Prepare a bounded set of encoded images without authenticating or uploading.
pub async fn prepare_images(
    client: &reqwest::Client,
    article: &Article,
    settings: &MediaConfig,
    http: &crate::config::HttpConfig,
) -> Result<Vec<PreparedImage>> {
    if !settings.enabled {
        return Ok(Vec::new());
    }
    let sources = discover(client, article, settings, http).await;
    let sources = match sources {
        Ok(sources) => sources,
        Err(error) if settings.required => return Err(error),
        Err(_) => article.image_urls.iter().take(32).cloned().collect(),
    };
    let mut images = Vec::new();
    for source in sources {
        if images.len() >= settings.max_images.min(4) {
            break;
        }
        let result = match crate::http::get_bytes_with_timeout(
            client,
            &source,
            settings.max_download_bytes,
            Duration::from_secs(http.timeout_seconds),
        )
        .await
        {
            Ok(bytes) => process(bytes, settings),
            Err(error) => Err(error),
        };
        match result {
            Ok(image) => images.push(image),
            Err(error) if settings.required => return Err(error),
            Err(_) => {}
        }
    }
    if settings.required && images.is_empty() {
        return Err(Error::Media("required media is unavailable".into()));
    }
    Ok(images)
}

/// Validate pixels before decoding, honor decoder budgets, then encode adaptively.
pub(crate) fn process(bytes: Vec<u8>, settings: &MediaConfig) -> Result<PreparedImage> {
    if settings.max_dimension == 0
        || settings.max_upload_bytes == 0
        || settings.max_source_pixels == 0
        || bytes.is_empty()
        || bytes.len() > settings.max_download_bytes
    {
        return Err(Error::Media(
            "source exceeds download budget or is empty".into(),
        ));
    }
    let format =
        image::guess_format(&bytes).map_err(|_| Error::Media("unsupported source image".into()))?;
    if !matches!(
        format,
        ImageFormat::Jpeg | ImageFormat::Png | ImageFormat::Gif | ImageFormat::WebP
    ) {
        return Err(Error::Media("unsupported source image".into()));
    }
    let (header_width, header_height) = header_dimensions(&bytes, format)?;
    if header_width == 0
        || header_height == 0
        || u64::from(header_width).saturating_mul(u64::from(header_height))
            > settings.max_source_pixels
    {
        return Err(Error::Media("source exceeds pixel budget".into()));
    }
    let mut limits = Limits::default();
    let axis_limit = u32::try_from(settings.max_source_pixels.min(u64::from(u32::MAX)))
        .map_err(|_| Error::Media("invalid pixel budget".into()))?;
    limits.max_image_width = Some(axis_limit);
    limits.max_image_height = Some(axis_limit);
    // The input cap plus pixel cap bound codec allocations. The library's max_alloc
    // is additionally enforced where supported; image documents it as best effort.
    limits.max_alloc = Some(
        settings
            .max_source_pixels
            .saturating_mul(8)
            .saturating_add(settings.max_download_bytes as u64),
    );
    let mut reader = ImageReader::with_format(Cursor::new(&bytes), format);
    reader.limits(limits);
    let mut decoder = reader
        .into_decoder()
        .map_err(|_| Error::Media("source decoder rejected image".into()))?;
    let (width, height) = decoder.dimensions();
    if (width, height) != (header_width, header_height)
        || width == 0
        || height == 0
        || u64::from(width).saturating_mul(u64::from(height)) > settings.max_source_pixels
    {
        return Err(Error::Media("source exceeds pixel budget".into()));
    }
    let orientation = decoder
        .exif_metadata()
        .map_err(|_| Error::Media("invalid image metadata".into()))?
        .as_deref()
        .and_then(Orientation::from_exif_chunk)
        .unwrap_or(
            decoder
                .orientation()
                .map_err(|_| Error::Media("invalid image orientation".into()))?,
        );
    // Decode even pass-through originals so corrupt/truncated sources never upload.
    let mut image = DynamicImage::from_decoder(decoder)
        .map_err(|_| Error::Media("source image is corrupt or exceeds allocation budget".into()))?;
    let max_dimension = settings.max_dimension.min(4000);
    let max_bytes = settings.max_upload_bytes.min(2_000_000);
    if orientation == Orientation::NoTransforms
        && width.max(height) <= max_dimension
        && bytes.len() <= max_bytes
        && matches!(format, ImageFormat::Jpeg | ImageFormat::Png)
    {
        return Ok(PreparedImage {
            bytes,
            mime: if format == ImageFormat::Jpeg {
                "image/jpeg".into()
            } else {
                "image/png".into()
            },
            width,
            height,
        });
    }
    drop(bytes);
    image.apply_orientation(orientation);
    if image.width().max(image.height()) > max_dimension {
        image = resize(image, max_dimension, max_dimension)?;
    }
    let alpha = image.color().has_alpha();
    loop {
        if alpha {
            if let Some(bytes) = encode_png(&image, max_bytes)? {
                return Ok(PreparedImage {
                    bytes,
                    mime: "image/png".into(),
                    width: image.width(),
                    height: image.height(),
                });
            }
        } else {
            // Prefer useful detail: try high quality at this resolution before
            // reducing dimensions, rather than forcing a low-quality fixed JPEG.
            let rgb = match image.as_rgb8() {
                Some(rgb) => Cow::Borrowed(rgb),
                None => Cow::Owned(image.to_rgb8()),
            };
            if let Some(bytes) = encode_highest_quality_jpeg(&rgb, max_bytes) {
                return Ok(PreparedImage {
                    bytes,
                    mime: "image/jpeg".into(),
                    width: rgb.width(),
                    height: rgb.height(),
                });
            }
        }
        let (width, height) = (image.width(), image.height());
        if width.max(height) <= 64 {
            return Err(Error::Media(
                "cannot encode image within upload budget".into(),
            ));
        }
        image = resize(
            image,
            ((width as f64 * 0.85).floor() as u32).max(1),
            ((height as f64 * 0.85).floor() as u32).max(1),
        )?;
    }
}

// Integer convolution buffers avoid the image crate's full floating-point
// intermediate. Consume the source so alpha can be premultiplied in place.
fn resize(image: DynamicImage, max_width: u32, max_height: u32) -> Result<DynamicImage> {
    let source_width = image.width();
    let source_height = image.height();
    let max_width = max_width.min(source_width).max(1);
    let max_height = max_height.min(source_height).max(1);
    let (width, height) = if u64::from(source_width) * u64::from(max_height)
        > u64::from(source_height) * u64::from(max_width)
    {
        (
            max_width,
            (u64::from(source_height) * u64::from(max_width) / u64::from(source_width)).max(1)
                as u32,
        )
    } else {
        (
            (u64::from(source_width) * u64::from(max_height) / u64::from(source_height)).max(1)
                as u32,
            max_height,
        )
    };
    if (width, height) == (source_width, source_height) {
        return Ok(image);
    }
    let alpha = image.color().has_alpha();
    let mut source = if alpha {
        DynamicImage::ImageRgba8(image.into_rgba8())
    } else {
        DynamicImage::ImageRgb8(image.into_rgb8())
    };
    let mut destination = if alpha {
        DynamicImage::new_rgba8(width, height)
    } else {
        DynamicImage::new_rgb8(width, height)
    };
    let alpha_math = fast_image_resize::MulDiv::new();
    if alpha {
        alpha_math
            .multiply_alpha_inplace(&mut source)
            .map_err(|_| Error::Media("image alpha normalization failed".into()))?;
    }
    let options = fast_image_resize::ResizeOptions::new()
        .resize_alg(fast_image_resize::ResizeAlg::Convolution(
            fast_image_resize::FilterType::Lanczos3,
        ))
        .use_alpha(false);
    let mut resizer = fast_image_resize::Resizer::new();
    resizer
        .resize(&source, &mut destination, Some(&options))
        .map_err(|_| Error::Media("image resize failed".into()))?;
    drop(resizer);
    drop(source);
    if alpha {
        alpha_math
            .divide_alpha_inplace(&mut destination)
            .map_err(|_| Error::Media("image alpha normalization failed".into()))?;
    }
    Ok(destination)
}

// Reject hostile dimensions before codec constructors can allocate scanlines,
// metadata buffers, or animation canvases. All indexing is checked.
fn header_dimensions(bytes: &[u8], format: ImageFormat) -> Result<(u32, u32)> {
    let invalid = || Error::Media("source image header is invalid".into());
    let be32 = |part: &[u8]| -> Result<u32> {
        Ok(u32::from_be_bytes(part.try_into().map_err(|_| invalid())?))
    };
    let le24 = |part: &[u8]| -> Result<u32> {
        let part: &[u8; 3] = part.try_into().map_err(|_| invalid())?;
        Ok(u32::from(part[0]) | u32::from(part[1]) << 8 | u32::from(part[2]) << 16)
    };
    match format {
        ImageFormat::Png => {
            if bytes.get(12..16) != Some(b"IHDR") {
                return Err(invalid());
            }
            Ok((
                be32(bytes.get(16..20).ok_or_else(invalid)?)?,
                be32(bytes.get(20..24).ok_or_else(invalid)?)?,
            ))
        }
        ImageFormat::Gif => {
            let width = u16::from_le_bytes(
                bytes
                    .get(6..8)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?,
            );
            let height = u16::from_le_bytes(
                bytes
                    .get(8..10)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?,
            );
            validate_gif_first_frame(bytes, width, height)?;
            Ok((u32::from(width), u32::from(height)))
        }
        ImageFormat::Jpeg => {
            let mut offset = 2;
            while offset < bytes.len() {
                if bytes[offset] != 0xff {
                    return Err(invalid());
                }
                while bytes.get(offset) == Some(&0xff) {
                    offset += 1;
                }
                let marker = *bytes.get(offset).ok_or_else(invalid)?;
                offset += 1;
                if marker == 0xda || marker == 0xd9 {
                    return Err(invalid());
                }
                if marker == 0x01 || (0xd0..=0xd8).contains(&marker) {
                    continue;
                }
                let length = u16::from_be_bytes(
                    bytes
                        .get(offset..offset + 2)
                        .ok_or_else(invalid)?
                        .try_into()
                        .map_err(|_| invalid())?,
                ) as usize;
                if length < 2 || length > bytes.len().saturating_sub(offset) {
                    return Err(invalid());
                }
                if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
                    if length < 7 {
                        return Err(invalid());
                    }
                    let height = u16::from_be_bytes(
                        bytes[offset + 3..offset + 5]
                            .try_into()
                            .map_err(|_| invalid())?,
                    );
                    let width = u16::from_be_bytes(
                        bytes[offset + 5..offset + 7]
                            .try_into()
                            .map_err(|_| invalid())?,
                    );
                    return Ok((u32::from(width), u32::from(height)));
                }
                offset += length;
            }
            Err(invalid())
        }
        ImageFormat::WebP => {
            let mut offset: usize = 12;
            while offset.saturating_add(8) <= bytes.len() {
                let tag = &bytes[offset..offset + 4];
                let length = u32::from_le_bytes(
                    bytes[offset + 4..offset + 8]
                        .try_into()
                        .map_err(|_| invalid())?,
                ) as usize;
                offset += 8;
                let chunk = bytes
                    .get(offset..offset.checked_add(length).ok_or_else(invalid)?)
                    .ok_or_else(invalid)?;
                if tag == b"VP8X" {
                    return Ok((
                        le24(chunk.get(4..7).ok_or_else(invalid)?)? + 1,
                        le24(chunk.get(7..10).ok_or_else(invalid)?)? + 1,
                    ));
                }
                if tag == b"VP8 " {
                    if chunk.get(3..6) != Some(&[0x9d, 0x01, 0x2a]) {
                        return Err(invalid());
                    }
                    let width = u16::from_le_bytes(
                        chunk
                            .get(6..8)
                            .ok_or_else(invalid)?
                            .try_into()
                            .map_err(|_| invalid())?,
                    ) & 0x3fff;
                    let height = u16::from_le_bytes(
                        chunk
                            .get(8..10)
                            .ok_or_else(invalid)?
                            .try_into()
                            .map_err(|_| invalid())?,
                    ) & 0x3fff;
                    return Ok((u32::from(width), u32::from(height)));
                }
                if tag == b"VP8L" {
                    if chunk.first() != Some(&0x2f) {
                        return Err(invalid());
                    }
                    let bits = u32::from_le_bytes(
                        chunk
                            .get(1..5)
                            .ok_or_else(invalid)?
                            .try_into()
                            .map_err(|_| invalid())?,
                    );
                    return Ok(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1));
                }
                offset = offset
                    .checked_add(length + (length % 2))
                    .ok_or_else(invalid)?;
            }
            Err(invalid())
        }
        _ => Err(invalid()),
    }
}

fn validate_gif_first_frame(bytes: &[u8], width: u16, height: u16) -> Result<()> {
    let invalid = || Error::Media("GIF frame is invalid or exceeds canvas".into());
    let packed = *bytes.get(10).ok_or_else(invalid)?;
    let mut offset = 13
        + if packed & 0x80 != 0 {
            3 << ((packed & 7) + 1)
        } else {
            0
        };
    loop {
        match bytes.get(offset) {
            Some(0x21) => {
                offset += 2;
                loop {
                    let size = usize::from(*bytes.get(offset).ok_or_else(invalid)?);
                    offset += 1;
                    if size == 0 {
                        break;
                    }
                    offset = offset
                        .checked_add(size)
                        .filter(|offset| *offset <= bytes.len())
                        .ok_or_else(invalid)?;
                }
            }
            Some(0x2c) => {
                let descriptor = bytes.get(offset + 1..offset + 10).ok_or_else(invalid)?;
                let left = u16::from_le_bytes([descriptor[0], descriptor[1]]);
                let top = u16::from_le_bytes([descriptor[2], descriptor[3]]);
                let frame_width = u16::from_le_bytes([descriptor[4], descriptor[5]]);
                let frame_height = u16::from_le_bytes([descriptor[6], descriptor[7]]);
                if frame_width == 0
                    || frame_height == 0
                    || u32::from(left) + u32::from(frame_width) > u32::from(width)
                    || u32::from(top) + u32::from(frame_height) > u32::from(height)
                {
                    return Err(invalid());
                }
                return Ok(());
            }
            _ => return Err(invalid()),
        }
    }
}

fn encode_jpeg(image: &image::RgbImage, quality: u8, max_bytes: usize) -> Option<Vec<u8>> {
    let mut writer = BoundedWriter::new(max_bytes);
    JpegEncoder::new_with_quality(&mut writer, quality)
        .encode(
            image.as_raw(),
            image.width(),
            image.height(),
            image::ExtendedColorType::Rgb8,
        )
        .ok()?;
    Some(writer.bytes)
}

fn encode_highest_quality_jpeg(image: &image::RgbImage, max_bytes: usize) -> Option<Vec<u8>> {
    if let Some(bytes) = encode_jpeg(image, 100, max_bytes) {
        return Some(bytes);
    }
    let mut best = encode_jpeg(image, 80, max_bytes)?;
    let (mut low, mut high) = (81_u8, 99_u8);
    while low <= high {
        let quality = low + (high - low) / 2;
        if let Some(bytes) = encode_jpeg(image, quality, max_bytes) {
            best = bytes;
            low = quality + 1;
        } else {
            high = quality - 1;
        }
    }
    Some(best)
}

fn encode_png(image: &DynamicImage, max_bytes: usize) -> Result<Option<Vec<u8>>> {
    let rgba = match image.as_rgba8() {
        Some(rgba) => Cow::Borrowed(rgba),
        None => Cow::Owned(image.to_rgba8()),
    };
    let mut writer = BoundedWriter::new(max_bytes);
    let encoded =
        PngEncoder::new_with_quality(&mut writer, CompressionType::Best, FilterType::Adaptive)
            .write_image(
                rgba.as_raw(),
                rgba.width(),
                rgba.height(),
                image::ExtendedColorType::Rgba8,
            );
    Ok(encoded.ok().map(|()| writer.bytes))
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl BoundedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
}
impl Write for BoundedWriter {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        if input.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("encoded image exceeds budget"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Validate a delivery key before using it as a local path.
pub fn directory(root: &Path, rkey: &str) -> Result<PathBuf> {
    rkey.parse::<atrium_api::types::string::Tid>()
        .map_err(|_| Error::Protocol("post key is not a TID".into()))?;
    Ok(root.join(rkey))
}

/// Cache one exact upload and its frozen embed before a record is written.
pub fn cache(
    root: &Path,
    rkey: &str,
    index: usize,
    prepared: &PreparedImage,
    image: &Value,
) -> Result<()> {
    if index >= 4 {
        return Err(Error::Media("too many images".into()));
    }
    let dir = directory(root, rkey)?;
    fs::create_dir_all(&dir)?;
    fs::write(dir.join(format!("{index}.image")), &prepared.bytes)?;
    let metadata = CachedImage {
        mime: prepared.mime.clone(),
        image: image.clone(),
    };
    let mut file = fs::File::create(dir.join(format!("{index}.json")))?;
    file.write_all(&serde_json::to_vec(&metadata)?)?;
    file.sync_all()?;
    fs::File::open(dir.join(format!("{index}.image")))?.sync_all()?;
    fs::File::open(dir)?.sync_all()?;
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

/// Reload one exact encoded image with a strict file-size ceiling.
pub fn load(
    root: &Path,
    rkey: &str,
    index: usize,
    expected: &Value,
) -> Result<(CachedImage, Vec<u8>)> {
    let dir = directory(root, rkey)?;
    let metadata: CachedImage =
        serde_json::from_slice(&read_limited(&dir.join(format!("{index}.json")), 32_768)?)
            .map_err(|_| Error::Media("cached image metadata is corrupt".into()))?;
    if &metadata.image != expected || !matches!(metadata.mime.as_str(), "image/jpeg" | "image/png")
    {
        return Err(Error::Media(
            "cached image does not match frozen record".into(),
        ));
    }
    let bytes = read_limited(&dir.join(format!("{index}.image")), 2_000_000)?;
    Ok((metadata, bytes))
}

fn read_limited(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(Error::Media("cached file exceeds budget".into()));
    }
    Ok(bytes)
}

/// Remove a single delivery's cache only after confirmed remote delivery.
pub fn cleanup(root: &Path, rkey: &str) -> Result<()> {
    let dir = directory(root, rkey)?;
    match fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{Rgb, RgbImage, Rgba, RgbaImage};

    fn png(width: u32, height: u32) -> Result<Vec<u8>> {
        let image = RgbImage::from_pixel(width, height, Rgb([35, 105, 180]));
        let mut bytes = Vec::new();
        PngEncoder::new(&mut bytes)
            .write_image(
                image.as_raw(),
                width,
                height,
                image::ExtendedColorType::Rgb8,
            )
            .map_err(|_| Error::Media("test encoding failed".into()))?;
        Ok(bytes)
    }

    #[test]
    fn preserves_exact_valid_original_at_dimension_and_byte_limits() -> Result<()> {
        let mut bytes = png(4000, 3)?;
        // PNG permits trailing data; padded fixture exercises the exact byte cap.
        bytes.resize(2_000_000, 0);
        let prepared = process_image(&bytes, &MediaConfig::default())?;
        assert_eq!(prepared.bytes, bytes);
        assert_eq!((prepared.width, prepared.height), (4000, 3));
        assert_eq!(prepared.mime, "image/png");
        Ok(())
    }

    #[test]
    fn resizes_without_upscaling_or_changing_aspect() -> Result<()> {
        let prepared = process_image(&png(5000, 2500)?, &MediaConfig::default())?;
        assert_eq!((prepared.width, prepared.height), (4000, 2000));
        assert!(prepared.bytes.len() <= 2_000_000);
        let small = process_image(&png(17, 9)?, &MediaConfig::default())?;
        assert_eq!((small.width, small.height), (17, 9));
        Ok(())
    }

    #[test]
    fn adaptive_conversion_obeys_size_budget_and_preserves_transparency() -> Result<()> {
        let config = MediaConfig {
            max_upload_bytes: 24_000,
            ..MediaConfig::default()
        };
        let image = RgbaImage::from_fn(250, 250, |x, y| {
            let n = x
                .wrapping_mul(747796405)
                .wrapping_add(y.wrapping_mul(2891336453));
            Rgba([
                (n >> 24) as u8,
                (n >> 16) as u8,
                (n >> 8) as u8,
                (x + y) as u8,
            ])
        });
        let mut bytes = Vec::new();
        PngEncoder::new(&mut bytes)
            .write_image(
                image.as_raw(),
                image.width(),
                image.height(),
                image::ExtendedColorType::Rgba8,
            )
            .map_err(|_| Error::Media("test encoding failed".into()))?;
        let prepared = process_image(&bytes, &config)?;
        assert!(prepared.bytes.len() <= config.max_upload_bytes);
        assert_eq!(prepared.mime, "image/png");
        assert!(
            image::load_from_memory(&prepared.bytes)
                .map_err(|_| Error::Media("test decoding failed".into()))?
                .color()
                .has_alpha()
        );
        Ok(())
    }

    #[test]
    fn converts_opaque_over_budget_original_adaptively() -> Result<()> {
        let config = MediaConfig {
            max_upload_bytes: 16_000,
            ..MediaConfig::default()
        };
        let image = RgbImage::from_fn(500, 300, |x, y| {
            let n = x
                .wrapping_mul(747796405)
                .wrapping_add(y.wrapping_mul(2891336453));
            Rgb([(n >> 24) as u8, (n >> 16) as u8, (n >> 8) as u8])
        });
        let mut bytes = Vec::new();
        PngEncoder::new(&mut bytes)
            .write_image(image.as_raw(), 500, 300, image::ExtendedColorType::Rgb8)
            .map_err(|_| Error::Media("test encoding failed".into()))?;
        let prepared = process_image(&bytes, &config)?;
        assert!(prepared.bytes.len() <= config.max_upload_bytes);
        assert_eq!(prepared.mime, "image/jpeg");
        assert!(prepared.width <= 500 && prepared.height <= 300);
        Ok(())
    }

    #[test]
    fn rejects_corrupt_sources_and_oversized_headers_before_decode() -> Result<()> {
        assert!(process_image(b"not an image", &MediaConfig::default()).is_err());
        let mut huge = png(10, 10)?;
        huge[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
        huge[20..24].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(
            matches!(process_image(&huge, &MediaConfig::default()), Err(Error::Media(message)) if message == "source exceeds pixel budget")
        );
        let mut truncated = png(20, 20)?;
        truncated.truncate(truncated.len() / 2);
        assert!(process_image(&truncated, &MediaConfig::default()).is_err());
        Ok(())
    }

    #[test]
    fn normalizes_jpeg_exif_display_orientation() -> Result<()> {
        let image = RgbImage::from_pixel(40, 20, Rgb([80, 30, 190]));
        let mut bytes = Vec::new();
        JpegEncoder::new_with_quality(&mut bytes, 95)
            .encode(image.as_raw(), 40, 20, image::ExtendedColorType::Rgb8)
            .map_err(|_| Error::Media("test encoding failed".into()))?;
        // Little-endian TIFF Orientation=6 (rotate 90 degrees clockwise).
        let exif = b"Exif\0\0II\x2a\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0\x06\0\0\0\0\0\0\0";
        let mut oriented = bytes[..2].to_vec();
        oriented.extend_from_slice(&[0xff, 0xe1]);
        oriented.extend_from_slice(&((exif.len() + 2) as u16).to_be_bytes());
        oriented.extend_from_slice(exif);
        oriented.extend_from_slice(&bytes[2..]);
        let prepared = process_image(&oriented, &MediaConfig::default())?;
        assert_eq!((prepared.width, prepared.height), (20, 40));
        assert_ne!(prepared.bytes, oriented);
        Ok(())
    }

    #[test]
    fn static_gif_and_webp_convert_to_png() -> Result<()> {
        let image = RgbaImage::from_pixel(40, 20, Rgba([80, 30, 190, 255]));
        let mut gif = Vec::new();
        image::codecs::gif::GifEncoder::new(&mut gif)
            .encode(image.as_raw(), 40, 20, image::ExtendedColorType::Rgba8)
            .map_err(|_| Error::Media("test GIF encoding failed".into()))?;
        let mut webp = Vec::new();
        image::codecs::webp::WebPEncoder::new_lossless(&mut webp)
            .write_image(image.as_raw(), 40, 20, image::ExtendedColorType::Rgba8)
            .map_err(|_| Error::Media("test WebP encoding failed".into()))?;
        for bytes in [&gif, &webp] {
            let prepared = process_image(bytes, &MediaConfig::default())?;
            assert_eq!((prepared.width, prepared.height), (40, 20));
            assert_eq!(prepared.mime, "image/png");
        }
        Ok(())
    }

    #[test]
    fn exact_cache_rejects_changed_frozen_embed_and_limits_cleanup_to_delivery() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let rkey = "3m4nz6k6wv222";
        let prepared = process_image(&png(20, 10)?, &MediaConfig::default())?;
        let image = serde_json::json!({"alt":"source photo", "image": {"ref":"fixture"}});
        cache(dir.path(), rkey, 0, &prepared, &image)?;
        assert_eq!(load(dir.path(), rkey, 0, &image)?.1, prepared.bytes);
        assert!(load(dir.path(), rkey, 0, &serde_json::json!({"alt":"changed"})).is_err());
        let unrelated = dir.path().join("unrelated");
        fs::create_dir(&unrelated)?;
        cleanup(dir.path(), rkey)?;
        assert!(unrelated.exists());
        assert!(directory(dir.path(), "../escape").is_err());
        Ok(())
    }
    #[test]
    fn jpeg_search_selects_highest_fitting_quality() -> Result<()> {
        let image = RgbImage::from_fn(60, 40, |x, y| {
            let n = x
                .wrapping_mul(747796405)
                .wrapping_add(y.wrapping_mul(2891336453));
            Rgb([(n >> 24) as u8, (n >> 16) as u8, (n >> 8) as u8])
        });
        let target = encode_jpeg(&image, 92, 2_000_000)
            .ok_or_else(|| Error::Media("test encoding failed".into()))?;
        let budget = target.len();
        let expected = (80..=100)
            .rev()
            .find_map(|quality| encode_jpeg(&image, quality, budget));
        assert_eq!(encode_highest_quality_jpeg(&image, budget), expected);
        Ok(())
    }

    #[tokio::test]
    async fn prefers_article_originals_over_feed_thumbnails_even_when_limit_is_one() -> Result<()> {
        use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};
        let server = MockServer::start().await;
        Mock::given(path("/article")).respond_with(ResponseTemplate::new(200).set_body_string(
            "<html><head><meta property='og:image' content='/og.jpg'><meta name='twitter:image' content='/twitter.png'></head><body><article><img src='/og.jpg'><img src='/article.webp'></article></body></html>"))
            .mount(&server).await;
        let article = Article {
            feed_id: "test".into(),
            id: "item".into(),
            aliases: vec![],
            url: format!("{}/article", server.uri()),
            title: "Source".into(),
            summary: String::new(),
            published_at: None,
            image_urls: vec!["/feed.png".into()],
            image_alt: None,
            feed_title: "Feed".into(),
        };
        let http = crate::config::HttpConfig::default();
        let sources = discover(
            &crate::http::client(&http)?,
            &article,
            &MediaConfig {
                max_images: 1,
                ..MediaConfig::default()
            },
            &http,
        )
        .await?;
        assert_eq!(
            sources,
            vec![
                format!("{}/og.jpg", server.uri()),
                format!("{}/twitter.png", server.uri()),
                format!("{}/article.webp", server.uri()),
                format!("{}/feed.png", server.uri())
            ]
        );
        Ok(())
    }
    fn discovery_article(server: &wiremock::MockServer) -> Article {
        Article {
            feed_id: "test".into(),
            id: "item".into(),
            aliases: vec![],
            url: format!("{}/article", server.uri()),
            title: "Source".into(),
            summary: String::new(),
            published_at: None,
            image_urls: vec!["/feed-thumbnail.jpg".into()],
            image_alt: None,
            feed_title: "Feed".into(),
        }
    }

    #[tokio::test]
    async fn ranks_largest_responsive_and_lazy_article_sources_before_feed_fallbacks() -> Result<()>
    {
        use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};
        let server = MockServer::start().await;
        Mock::given(path("/article")).respond_with(ResponseTemplate::new(200).set_body_string(
            "<article><img width='400' src='/thumb-small.jpg' srcset='/small-400.jpg 400w, /original-1200.webp 1200w'><img width='700' src='data:image/gif;base64,placeholder' data-src='/lazy-original.jpg' data-srcset='/lazy-800.jpg 800w, /lazy-2000.webp 2000w'><picture><source srcset='/density-one.png 1x, /density-three.webp 3x' width='500'></picture></article>"))
            .mount(&server).await;
        let article = discovery_article(&server);
        let http = crate::config::HttpConfig::default();
        let sources = discover(
            &crate::http::client(&http)?,
            &article,
            &MediaConfig::default(),
            &http,
        )
        .await?;
        assert_eq!(sources[0], format!("{}/lazy-2000.webp", server.uri()));
        assert_eq!(sources[1], format!("{}/density-three.webp", server.uri()));
        assert_eq!(sources[2], format!("{}/original-1200.webp", server.uri()));
        assert!(
            sources
                .iter()
                .any(|source| source.ends_with("/lazy-original.jpg"))
        );
        assert_eq!(
            sources.last(),
            Some(&format!("{}/feed-thumbnail.jpg", server.uri()))
        );
        assert!(
            !sources
                .iter()
                .any(|source| source.ends_with("/small-400.jpg")
                    || source.ends_with("/lazy-800.jpg"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn article_discovery_failure_retains_valid_feed_images() -> Result<()> {
        use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};
        let server = MockServer::start().await;
        Mock::given(path("/article"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        let article = discovery_article(&server);
        let http = crate::config::HttpConfig::default();
        let sources = discover(
            &crate::http::client(&http)?,
            &article,
            &MediaConfig {
                required: true,
                max_images: 1,
                ..MediaConfig::default()
            },
            &http,
        )
        .await?;
        assert_eq!(
            sources,
            vec![format!("{}/feed-thumbnail.jpg", server.uri())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn feed_only_discovery_avoids_article_requests() -> Result<()> {
        use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};
        let server = MockServer::start().await;
        Mock::given(path("/article"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let article = discovery_article(&server);
        let http = crate::config::HttpConfig::default();
        let sources = discover(
            &crate::http::client(&http)?,
            &article,
            &MediaConfig {
                discover_from_article: false,
                ..MediaConfig::default()
            },
            &http,
        )
        .await?;
        assert_eq!(
            sources,
            vec![format!("{}/feed-thumbnail.jpg", server.uri())]
        );
        Ok(())
    }
}
