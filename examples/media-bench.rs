//! Offline fixture generation and measurement through the production image pipeline.
//! Generate in a separate process, then measure `process` with /usr/bin/time.

use anyhow::{Context, Result, bail};
use image::{Rgb, RgbImage, codecs::jpeg::JpegEncoder};
use rss_bluesky_bot::{config::MediaConfig, media};
use std::{env, fs, path::Path};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [command, filename, width, height] if command == "generate" => {
            let width: u32 = width.parse()?;
            let height: u32 = height.parse()?;
            if width == 0 || height == 0 || u64::from(width) * u64::from(height) > 24_000_000 {
                bail!("Fixture must contain 1–24000000 pixels");
            }
            let mut seed = 123456789_u32;
            let image = RgbImage::from_fn(width, height, |x, y| {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                let luminance = (seed % 128 + ((x / 16 + y / 16) % 64)) as u8;
                Rgb([luminance, luminance, luminance])
            });
            let mut file = fs::File::create(Path::new(filename))?;
            JpegEncoder::new_with_quality(&mut file, 90).encode_image(&image)?;
            println!(
                "{}",
                serde_json::json!({"fixture":filename,"width":width,"height":height,
                "source_bytes":fs::metadata(filename)?.len()})
            );
        }
        [command, filename] if command == "process" => {
            let bytes = fs::read(filename).context("Cannot read fixture")?;
            let prepared = media::process_image(&bytes, &MediaConfig::default())?;
            println!(
                "{}",
                serde_json::json!({"width":prepared.width,"height":prepared.height,
                "source_bytes":bytes.len(),"upload_bytes":prepared.bytes.len(),"mime":prepared.mime})
            );
        }
        _ => bail!("Use media-bench generate PATH WIDTH HEIGHT, or media-bench process PATH"),
    }
    Ok(())
}
