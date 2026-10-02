//! Reusable transport with bounded decompressed bodies and redacted failures.

use std::io::Read;
use std::time::Duration;

use reqwest::{Client, Response, StatusCode};

use crate::config::{HttpConfig, web_url};
use crate::{Error, Result};

/// Construct a shared HTTP client with a total request deadline and bounded redirects.
pub fn client(settings: &HttpConfig) -> Result<Client> {
    if !(1..=600).contains(&settings.timeout_seconds) {
        return Err(Error::Config(
            "timeout_seconds must be between 1 and 600".into(),
        ));
    }
    Client::builder()
        .gzip(false)
        .timeout(Duration::from_secs(settings.timeout_seconds))
        .connect_timeout(Duration::from_secs(settings.timeout_seconds.min(10)))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let target = attempt.url();
            if attempt.previous().len() >= 5
                || !matches!(target.scheme(), "http" | "https")
                || !target.username().is_empty()
                || target.password().is_some()
            {
                attempt.error("redirect policy rejected response")
            } else {
                attempt.follow()
            }
        }))
        .user_agent(concat!("rss-bluesky-bot/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(transport_error)
}

/// Fetch at most `limit` decompressed bytes, rejecting unsuccessful HTTP responses.
pub async fn get_bytes(client: &Client, url: &str, limit: usize) -> Result<Vec<u8>> {
    get_bytes_with_timeout(client, url, limit, Duration::from_secs(30)).await
}

/// Fetch source bytes with a total deadline covering transport and gzip decoding.
pub async fn get_bytes_with_timeout(
    client: &Client,
    url: &str,
    limit: usize,
    timeout: Duration,
) -> Result<Vec<u8>> {
    let url = web_url(url).map_err(|_| Error::Transport("invalid source URL".into()))?;
    tokio::time::timeout(timeout, async {
        let response = client
            .get(url)
            .header(reqwest::header::ACCEPT_ENCODING, "gzip")
            .send()
            .await
            .map_err(transport_error)?;
        response_bytes(response, limit).await
    })
    .await
    .map_err(|_| Error::Transport("request timed out".into()))?
}

pub(crate) async fn response_bytes(mut response: Response, limit: usize) -> Result<Vec<u8>> {
    if !response.status().is_success() {
        return Err(status_error(&response));
    }
    if limit == 0
        || response
            .content_length()
            .is_some_and(|length| length > limit as u64)
    {
        return Err(Error::Transport(
            "response exceeds configured body limit".into(),
        ));
    }
    let encoding = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| Error::Transport("invalid response content encoding".into()))?;
    let compressed = match encoding.map(str::trim) {
        None => false,
        Some(value) if value.eq_ignore_ascii_case("identity") => false,
        Some(value)
            if value.eq_ignore_ascii_case("gzip") || value.eq_ignore_ascii_case("x-gzip") =>
        {
            true
        }
        _ => {
            return Err(Error::Transport(
                "unsupported response content encoding".into(),
            ));
        }
    };
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(Error::Transport(
                "response exceeds configured body limit".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if !compressed {
        return Ok(bytes);
    }
    let mut decoder = flate2::read::MultiGzDecoder::new(bytes.as_slice());
    let mut decoded = Vec::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let length = decoder
            .read(&mut buffer)
            .map_err(|_| Error::Transport("invalid compressed response body".into()))?;
        if length == 0 {
            return Ok(decoded);
        }
        if length > limit.saturating_sub(decoded.len()) {
            return Err(Error::Transport(
                "response exceeds configured body limit".into(),
            ));
        }
        decoded.extend_from_slice(&buffer[..length]);
        // Cooperative decoding lets the total deadline cancel decompression, too.
        tokio::task::yield_now().await;
    }
}

pub(crate) fn status_error(response: &Response) -> Error {
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value.parse::<u64>().ok().or_else(|| {
                chrono::DateTime::parse_from_rfc2822(value)
                    .ok()
                    .map(|date| {
                        (date.with_timezone(&chrono::Utc) - chrono::Utc::now())
                            .num_seconds()
                            .max(0) as u64
                    })
            })
        });
    Error::Http {
        status: response.status().as_u16(),
        retry_after,
    }
}

pub(crate) fn transport_error(error: reqwest::Error) -> Error {
    let category = if error.is_timeout() {
        "request timed out"
    } else if error.is_connect() {
        "connection failed"
    } else if error.is_redirect() {
        "redirect rejected"
    } else if error.is_body() || error.is_decode() {
        "response body failed"
    } else {
        "request failed"
    };
    Error::Transport(category.into())
}

/// Whether a response is a valid conditional-fetch cache hit.
pub(crate) fn not_modified(response: &Response) -> bool {
    response.status() == StatusCode::NOT_MODIFIED
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tokio::io::AsyncWriteExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn rejects_oversized_bodies_and_accepts_exact_limit() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/body"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 32]))
            .mount(&server)
            .await;
        let transport = client(&HttpConfig::default())?;
        assert_eq!(
            get_bytes(&transport, &format!("{}/body", server.uri()), 32)
                .await?
                .len(),
            32
        );
        assert!(
            get_bytes(&transport, &format!("{}/body", server.uri()), 31)
                .await
                .is_err()
        );
        Ok(())
    }

    fn gzip(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes)?;
        Ok(encoder.finish()?)
    }

    #[tokio::test]
    async fn gzip_cap_applies_to_encoded_and_decoded_sizes() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(path("/bomb"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Encoding", "gzip")
                    .set_body_bytes(gzip(&vec![b'x'; 4096])?),
            )
            .mount(&server)
            .await;
        Mock::given(path("/encoded"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Encoding", "gzip")
                    .set_body_bytes(gzip(&(0u8..32).collect::<Vec<_>>())?),
            )
            .mount(&server)
            .await;
        let transport = client(&HttpConfig::default())?;
        assert!(
            get_bytes(&transport, &format!("{}/bomb", server.uri()), 256)
                .await
                .is_err()
        );
        assert_eq!(
            get_bytes(&transport, &format!("{}/bomb", server.uri()), 4096).await?,
            vec![b'x'; 4096]
        );
        assert!(
            get_bytes(&transport, &format!("{}/encoded", server.uri()), 32)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn chunked_response_without_content_length_remains_bounded() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n10\r\nxxxxxxxxxxxxxxxx\r\n10\r\nxxxxxxxxxxxxxxxx\r\n0\r\n\r\n").await
        });
        let transport = client(&HttpConfig::default())?;
        assert!(
            get_bytes(&transport, &format!("http://{address}/body"), 31)
                .await
                .is_err()
        );
        server
            .await
            .map_err(|_| Error::Transport("test server failed".into()))??;
        Ok(())
    }

    #[tokio::test]
    async fn total_deadline_applies_to_response_waiting() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(1))
                    .set_body_string("body"),
            )
            .mount(&server)
            .await;
        let transport = client(&HttpConfig::default())?;
        let result =
            get_bytes_with_timeout(&transport, &server.uri(), 32, Duration::from_millis(50)).await;
        assert!(matches!(result, Err(Error::Transport(message)) if message == "request timed out"));
        Ok(())
    }

    #[tokio::test]
    async fn rate_limit_error_preserves_retry_after_without_echoing_body_or_url() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "17")
                    .set_body_string("private response token"),
            )
            .mount(&server)
            .await;
        let transport = client(&HttpConfig::default())?;
        let result = get_bytes(
            &transport,
            &format!("{}/secret?token=private", server.uri()),
            32,
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::Http {
                status: 429,
                retry_after: Some(17)
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn network_errors_do_not_echo_source_urls() -> Result<()> {
        let transport = client(&HttpConfig::default())?;
        let result = get_bytes(&transport, "http://127.0.0.1:1/secret?token=private", 32).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(!error.to_string().contains("private"));
        }
        Ok(())
    }
}
