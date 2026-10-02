//! RSS and Atom normalization with stable identities and separate eligibility.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use feed_rs::model::{Entry, FeedType, Text};
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::Client;
use scraper::{Html, Selector};
use url::Url;

use crate::config::{FeedConfig, HttpConfig, web_url};
use crate::http;
use crate::model::{Article, FeedFetch, FeedValidators};
use crate::{Error, Result};

/// Parse every validated item, preserving identities even for ineligible articles.
///
/// A missing title becomes the summary or `Untitled`; dates and summaries may be
/// absent. An item requires an HTTP(S) article link and either a GUID or that link.
/// Any invalid item rejects the entire scan so it cannot establish a partial baseline.
pub fn parse(bytes: &[u8], feed: &FeedConfig) -> Result<Vec<Article>> {
    validate_xml(bytes)?;
    let source_url =
        web_url(&feed.url).map_err(|_| Error::Feed("invalid configured source URL".into()))?;
    let parsed = feed_rs::parser::Builder::new()
        .base_uri(Some(&feed.url))
        .id_generator(|_, _, _| String::new())
        .sanitize_content(false)
        .build()
        .parse(bytes)
        .map_err(|_| Error::Feed("malformed or unsupported RSS/Atom document".into()))?;
    if parsed.feed_type == FeedType::JSON {
        return Err(Error::Feed(
            "only RSS and Atom documents are supported".into(),
        ));
    }
    if parsed.entries.len() > 50_000 {
        return Err(Error::Feed("feed contains too many entries".into()));
    }
    let feed_title = parsed
        .title
        .as_ref()
        .map(normalize_text)
        .filter(|title| !title.is_empty())
        .unwrap_or_else(|| feed.id.clone());
    let mut articles: Vec<Article> = Vec::with_capacity(parsed.entries.len());
    let mut ids = HashMap::<String, usize>::new();
    for entry in parsed.entries {
        let article = normalize(entry, feed, &source_url, &feed_title)?;
        if let Some(&index) = ids.get(&article.id) {
            if articles[index].url != article.url {
                return Err(Error::Feed(
                    "duplicate entry ID has conflicting article links".into(),
                ));
            }
            continue;
        }
        ids.insert(article.id.clone(), articles.len());
        articles.push(article);
    }
    Ok(articles)
}

/// Evaluate inclusive UTC date and case-insensitive substring keyword filters.
///
/// Includes use any-match semantics; exclusions win. An undated item cannot pass
/// a configured date cutoff. This never changes the article's title or summary.
pub fn eligible(article: &Article, feed: &FeedConfig) -> bool {
    if feed.min_date.is_some_and(|cutoff| {
        article
            .published_at
            .is_none_or(|date| date.date_naive() < cutoff)
    }) {
        return false;
    }
    let haystack = format!("{}\n{}", article.title, article.summary).to_lowercase();
    let matches = |keyword: &String| haystack.contains(&keyword.trim().to_lowercase());
    (feed.include_keywords.is_empty() || feed.include_keywords.iter().any(matches))
        && !feed.exclude_keywords.iter().any(matches)
}

/// Exact, case-sensitive lookup variants for historical titles without source metadata.
///
/// Legacy rows may contain plain text or HTML that the current feed parser renders
/// as text. Keep both whitespace-normalized and HTML-rendered variants, without
/// changing punctuation, case, or the original stored title. Empty keys are omitted
/// so a blank or markup-only historical title cannot suppress unrelated articles.
pub fn legacy_title_keys(title: &str) -> Vec<String> {
    let mut keys = Vec::with_capacity(2);
    for key in [collapse_whitespace(title), plain_text(title)] {
        if !key.is_empty() {
            push_unique(&mut keys, key);
        }
    }
    keys
}

/// Fetch a feed conditionally, updating validators only alongside a valid full scan.
pub async fn fetch(
    client: &Client,
    feed: &FeedConfig,
    settings: &HttpConfig,
    validators: Option<&FeedValidators>,
) -> Result<FeedFetch> {
    if !(1..=600).contains(&settings.timeout_seconds) || settings.max_feed_bytes == 0 {
        return Err(Error::Config(
            "feed request limits must be positive and bounded".into(),
        ));
    }
    let source_url =
        web_url(&feed.url).map_err(|_| Error::Feed("invalid configured source URL".into()))?;
    let operation = async {
        let mut request = client
            .get(source_url)
            .header(reqwest::header::ACCEPT_ENCODING, "gzip");
        let mut conditional = false;
        if let Some(validators) = validators {
            for (name, value) in [
                (reqwest::header::IF_NONE_MATCH, &validators.etag),
                (
                    reqwest::header::IF_MODIFIED_SINCE,
                    &validators.last_modified,
                ),
            ] {
                if let Some(value) = value {
                    if value.len() > 8_192 {
                        return Err(Error::Feed(
                            "stored conditional validator is invalid".into(),
                        ));
                    }
                    let value = reqwest::header::HeaderValue::from_str(value).map_err(|_| {
                        Error::Feed("stored conditional validator is invalid".into())
                    })?;
                    request = request.header(name, value);
                    conditional = true;
                }
            }
        }
        let response = request.send().await.map_err(http::transport_error)?;
        if http::not_modified(&response) {
            return if conditional {
                Ok(FeedFetch::NotModified)
            } else {
                Err(Error::Feed(
                    "unexpected not-modified response without conditional validators".into(),
                ))
            };
        }
        let validator = |name| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .filter(|value| value.len() <= 8_192)
                .map(str::to_owned)
        };
        let validators = FeedValidators {
            etag: validator(reqwest::header::ETAG),
            last_modified: validator(reqwest::header::LAST_MODIFIED),
        };
        let bytes = http::response_bytes(response, settings.max_feed_bytes).await?;
        let articles = parse(&bytes, feed)?;
        Ok(FeedFetch::Modified {
            articles,
            validators,
        })
    };
    tokio::time::timeout(Duration::from_secs(settings.timeout_seconds), operation)
        .await
        .map_err(|_| Error::Transport("feed request timed out".into()))?
}

fn normalize(
    entry: Entry,
    feed: &FeedConfig,
    source_url: &Url,
    feed_title: &str,
) -> Result<Article> {
    let base = entry
        .base
        .as_deref()
        .and_then(|base| source_url.join(base).ok())
        .unwrap_or_else(|| source_url.clone());
    let mut links = Vec::new();
    for link in &entry.links {
        if link
            .rel
            .as_deref()
            .is_none_or(|relation| relation == "alternate")
        {
            if link.media_type.as_deref().is_some_and(|mime| {
                !matches!(
                    mime.split(';').next(),
                    Some("text/html" | "application/xhtml+xml")
                )
            }) {
                continue;
            }
            let url = article_url(&link.href, &base)
                .ok_or_else(|| Error::Feed("entry contains an invalid article link".into()))?;
            push_unique(&mut links, url);
            if links.len() > 256 {
                return Err(Error::Feed("entry contains too many article links".into()));
            }
        }
    }
    let guid = entry.id.trim();
    if guid.len() > 16_384 || guid.chars().any(char::is_control) {
        return Err(Error::Feed("entry ID is invalid".into()));
    }
    let guid_url = if guid
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
        || guid
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
    {
        Some(
            article_url(guid, &base)
                .ok_or_else(|| Error::Feed("entry GUID contains an invalid URL".into()))?,
        )
    } else {
        None
    };
    let url = links
        .first()
        .cloned()
        .or_else(|| guid_url.clone())
        .ok_or_else(|| Error::Feed("entry has no valid HTTP(S) article link".into()))?;
    let id = if guid.is_empty() {
        url.clone()
    } else {
        guid.to_owned()
    };
    let mut aliases = Vec::new();
    push_unique(&mut aliases, id.clone());
    for link in links {
        push_unique(&mut aliases, link);
    }
    if let Some(guid_url) = guid_url {
        push_unique(&mut aliases, guid_url);
    }
    push_unique(&mut aliases, url.clone());
    let summary = entry
        .summary
        .as_ref()
        .map(normalize_text)
        .filter(|text| !text.is_empty())
        .or_else(|| {
            entry
                .content
                .as_ref()
                .and_then(|content| content.body.as_deref())
                .map(plain_text)
        })
        .unwrap_or_default();
    let title = entry
        .title
        .as_ref()
        .map(normalize_text)
        .filter(|text| !text.is_empty())
        .or_else(|| (!summary.is_empty()).then(|| summary.clone()))
        .unwrap_or_else(|| "Untitled".into());
    let image_base = entry
        .base
        .as_deref()
        .and_then(|base| source_url.join(base).ok())
        .or_else(|| Url::parse(&url).ok())
        .unwrap_or(base);
    let (image_urls, image_alt) = images(&entry, &image_base)?;
    Ok(Article {
        feed_id: feed.id.clone(),
        id,
        aliases,
        url,
        title,
        summary,
        published_at: entry.published.or(entry.updated),
        image_urls,
        image_alt,
        feed_title: feed_title.into(),
    })
}

fn article_url(value: &str, base: &Url) -> Option<String> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return None;
    }
    let mut url = base.join(value.trim()).ok()?;
    web_url(url.as_str()).ok()?;
    url.set_fragment(None);
    Some(url.to_string())
}

fn images(entry: &Entry, base: &Url) -> Result<(Vec<String>, Option<String>)> {
    let mut urls = Vec::new();
    let mut alt = None;
    for media in &entry.media {
        for content in &media.content {
            if let Some(url) = &content.url
                && (content
                    .content_type
                    .as_ref()
                    .is_some_and(|mime| mime.to_string().starts_with("image/"))
                    || image_extension(url))
                && let Some(url) = article_url(url.as_str(), base)
            {
                push_image(&mut urls, url);
            }
        }
        for thumbnail in &media.thumbnails {
            if let Some(url) = article_url(&thumbnail.image.uri, base) {
                push_image(&mut urls, url);
                alt = alt.or_else(|| {
                    thumbnail
                        .image
                        .title
                        .as_deref()
                        .map(plain_text)
                        .filter(|value| !value.is_empty())
                });
            }
        }
        alt = alt.or_else(|| {
            media
                .description
                .as_ref()
                .or(media.title.as_ref())
                .map(normalize_text)
                .filter(|value| !value.is_empty())
        });
    }
    for link in &entry.links {
        if link.rel.as_deref() == Some("enclosure")
            && link
                .media_type
                .as_deref()
                .is_some_and(|mime| mime.starts_with("image/"))
            && let Some(url) = article_url(&link.href, base)
        {
            push_image(&mut urls, url);
        }
    }
    if let Some(content) = &entry.content
        && content.content_type.to_string().starts_with("image/")
        && let Some(source) = &content.src
        && let Some(url) = article_url(&source.href, base)
    {
        push_image(&mut urls, url);
    }
    let selector = Selector::parse("img")
        .map_err(|_| Error::Feed("image selector could not be constructed".into()))?;
    for html in entry
        .summary
        .as_ref()
        .map(|summary| summary.content.as_str())
        .into_iter()
        .chain(
            entry
                .content
                .as_ref()
                .and_then(|content| content.body.as_deref()),
        )
    {
        let fragment = Html::parse_fragment(html);
        for image in fragment.select(&selector) {
            let attributes = image.value();
            let responsive = attributes
                .attr("data-srcset")
                .or_else(|| attributes.attr("srcset"))
                .and_then(largest_srcset);
            for source in responsive
                .into_iter()
                .chain(attributes.attr("data-src"))
                .chain(attributes.attr("src"))
            {
                if let Some(url) = article_url(source, base) {
                    push_image(&mut urls, url);
                    alt = alt.or_else(|| {
                        attributes
                            .attr("alt")
                            .map(plain_text)
                            .filter(|value| !value.is_empty())
                    });
                }
            }
        }
    }
    Ok((urls, alt))
}

fn largest_srcset(srcset: &str) -> Option<&str> {
    srcset
        .split(',')
        .filter_map(|candidate| {
            let mut pieces = candidate.split_whitespace();
            let url = pieces.next()?;
            let size = pieces
                .next()
                .and_then(|descriptor| {
                    descriptor
                        .strip_suffix('w')
                        .or_else(|| descriptor.strip_suffix('x'))
                })
                .and_then(|size| size.parse::<f64>().ok())
                .filter(|size| size.is_finite() && *size > 0.0)
                .unwrap_or(1.0);
            Some((url, size))
        })
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
        .map(|(url, _)| url)
}

fn image_extension(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    [".jpg", ".jpeg", ".png", ".webp", ".gif"]
        .iter()
        .any(|extension| path.ends_with(extension))
}

fn normalize_text(text: &Text) -> String {
    if text.content_type.to_string().contains("html") {
        plain_text(&text.content)
    } else {
        collapse_whitespace(&text.content)
    }
}

fn plain_text(source: &str) -> String {
    let document = Html::parse_fragment(source);
    let mut words = Vec::new();
    for node in document.tree.root().descendants() {
        if let Some(text) = node.value().as_text()
            && !node.ancestors().any(|ancestor| {
                ancestor
                    .value()
                    .as_element()
                    .is_some_and(|element| matches!(element.name(), "script" | "style"))
            })
        {
            words.extend(text.split_whitespace());
        }
    }
    words.join(" ")
}

fn collapse_whitespace(source: &str) -> String {
    source.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn push_image(values: &mut Vec<String>, value: String) {
    if values.len() < 64 {
        push_unique(values, value);
    }
}

fn validate_xml(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Err(Error::Feed("empty feed response".into()));
    }
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().check_end_names = true;
    let mut depth = 0usize;
    let mut roots = 0usize;
    let mut root_supported = false;
    let mut buffer = Vec::new();
    loop {
        let event = reader
            .read_event_into(&mut buffer)
            .map_err(|_| Error::Feed("malformed XML document".into()))?;
        let empty = matches!(event, Event::Empty(_));
        match event {
            Event::Start(element) | Event::Empty(element) => {
                if depth == 0 {
                    roots += 1;
                    root_supported =
                        matches!(element.local_name().as_ref(), "rss" | "feed" | "RDF");
                }
                let mut attributes = HashSet::new();
                for attribute in element.attributes() {
                    let attribute =
                        attribute.map_err(|_| Error::Feed("malformed XML attributes".into()))?;
                    attribute
                        .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                        .map_err(|_| Error::Feed("invalid XML attribute value".into()))?;
                    if !attributes.insert(attribute.key.as_ref().to_owned()) {
                        return Err(Error::Feed("duplicate XML attribute".into()));
                    }
                }
                if !empty {
                    depth += 1;
                    if depth > 128 {
                        return Err(Error::Feed("XML nesting exceeds safe limit".into()));
                    }
                }
            }
            Event::End(_) => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| Error::Feed("malformed XML nesting".into()))?;
            }
            Event::DocType(_) => {
                return Err(Error::Feed("XML document types are unsupported".into()));
            }
            Event::GeneralRef(reference) => {
                if reference
                    .resolve_char_ref()
                    .map_err(|_| Error::Feed("invalid XML character reference".into()))?
                    .is_none()
                    && quick_xml::escape::resolve_predefined_entity(reference.as_ref()).is_none()
                {
                    return Err(Error::Feed("unknown XML entity reference".into()));
                }
            }
            Event::Text(text)
                if depth == 0 && !text.as_ref().bytes().all(|byte| byte.is_ascii_whitespace()) =>
            {
                return Err(Error::Feed("unexpected text outside XML root".into()));
            }
            Event::Eof => break,
            _ => {}
        }
        buffer.clear();
    }
    if depth != 0 || roots != 1 || !root_supported {
        return Err(Error::Feed(
            "feed requires one complete RSS/Atom XML root".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const RSS: &str = r#"<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel>
        <title>Science &amp; Nature</title><link>https://example.org/</link><description>News</description>
        <item><guid isPermaLink="false">stable-1</guid><title>New telescope</title><link>/news/one#section</link>
        <pubDate>Thu, 01 Oct 2026 00:00:00 +0000</pubDate><description><![CDATA[<p>A <b>bright</b> discovery.</p><img src="/images/one.jpg" alt="A telescope"/>]]></description>
        <media:content url="https://example.org/large.png" type="image/png"/></item>
        <item><guid isPermaLink="false">stable-2</guid><title>New telescope</title><link>/news/two</link></item>
    </channel></rss>"#;

    fn settings() -> FeedConfig {
        FeedConfig {
            id: "science".into(),
            url: "https://example.org/feeds/latest.xml".into(),
            ..FeedConfig::default()
        }
    }

    #[test]
    fn generic_rss_preserves_same_title_distinct_ids_and_media() -> Result<()> {
        let articles = parse(RSS.as_bytes(), &settings())?;
        assert_eq!(articles.len(), 2);
        assert_ne!(articles[0].id, articles[1].id);
        assert_eq!(articles[0].title, articles[1].title);
        assert_eq!(
            articles[0].aliases,
            vec!["stable-1", "https://example.org/news/one"]
        );
        assert_eq!(articles[0].summary, "A bright discovery.");
        assert_eq!(
            articles[0].image_urls,
            vec![
                "https://example.org/large.png",
                "https://example.org/images/one.jpg"
            ]
        );
        assert_eq!(articles[0].image_alt.as_deref(), Some("A telescope"));
        Ok(())
    }

    #[test]
    fn atom_resolves_xml_base_and_uses_updated_when_published_is_missing() -> Result<()> {
        let bytes = br#"<feed xmlns="http://www.w3.org/2005/Atom" xml:base="https://research.example/articles/"><id>urn:feed:research</id><title>Research</title><updated>2026-10-01T12:00:00Z</updated>
        <entry><id>urn:entry:42</id><title type="html">Fish &amp;amp; chips</title><updated>2026-10-01T12:00:00Z</updated><link rel="self" href="entry.xml"/><link rel="alternate" href="paper"/><summary type="html">&lt;p&gt;Paper summary&lt;/p&gt;</summary><link rel="enclosure" type="image/jpeg" href="photo.jpg"/></entry></feed>"#;
        let articles = parse(bytes, &settings())?;
        assert_eq!(articles[0].url, "https://research.example/articles/paper");
        assert_eq!(articles[0].title, "Fish & chips");
        assert_eq!(articles[0].summary, "Paper summary");
        assert_eq!(
            articles[0].image_urls,
            vec!["https://research.example/articles/photo.jpg"]
        );
        assert_eq!(
            articles[0].published_at,
            Utc.with_ymd_and_hms(2026, 10, 1, 12, 0, 0).single()
        );
        Ok(())
    }

    #[test]
    fn missing_guid_title_summary_and_date_are_handled_without_random_identity() -> Result<()> {
        let bytes = br#"<rss version="2.0"><channel><title>Example</title><item><link>https://example.org/item</link></item></channel></rss>"#;
        let articles = parse(bytes, &settings())?;
        assert_eq!(articles[0].id, "https://example.org/item");
        assert_eq!(articles[0].title, "Untitled");
        assert_eq!(articles[0].summary, "");
        assert_eq!(articles[0].published_at, None);
        Ok(())
    }

    #[test]
    fn missing_link_uses_url_guid_and_missing_title_uses_summary() -> Result<()> {
        let bytes = br#"<rss version="2.0"><channel><item><guid>https://example.org/item</guid><description>Summary title</description></item></channel></rss>"#;
        let articles = parse(bytes, &settings())?;
        assert_eq!(articles[0].url, "https://example.org/item");
        assert_eq!(articles[0].title, "Summary title");
        Ok(())
    }

    #[test]
    fn filters_are_inclusive_unicode_case_insensitive_and_do_not_mutate_source() -> Result<()> {
        let mut articles = parse(RSS.as_bytes(), &settings())?;
        articles[0].summary = "CAFÉ and science".into();
        let source = articles[0].clone();
        let mut config = settings();
        config.min_date = NaiveDate::from_ymd_opt(2026, 10, 1);
        config.include_keywords = vec![" café ".into(), "unmatched".into()];
        assert!(eligible(&articles[0], &config));
        assert!(!eligible(&articles[1], &config));
        config.exclude_keywords = vec!["SCIENCE".into()];
        assert!(!eligible(&articles[0], &config));
        config.exclude_keywords.clear();
        config.min_date = NaiveDate::from_ymd_opt(2026, 10, 2);
        assert!(!eligible(&articles[0], &config));
        assert_eq!(articles[0], source);
        Ok(())
    }

    #[test]
    fn legacy_title_keys_collapse_spaces_tabs_and_nonbreaking_spaces() {
        let title = "  New\t  housing\u{a0} near\n transit  ";
        assert_eq!(legacy_title_keys(title), vec!["New housing near transit"]);
        assert_eq!(title, "  New\t  housing\u{a0} near\n transit  ");
    }

    #[test]
    fn legacy_title_keys_decode_html_entities_and_match_feed_html_rendering() -> Result<()> {
        let legacy = "<strong>Caf&eacute;</strong>&nbsp;&amp; housing";
        let bytes = br#"<feed xmlns="http://www.w3.org/2005/Atom"><id>urn:example</id><title>Example</title><updated>2026-10-01T00:00:00Z</updated><entry><id>urn:entry</id><title type="html">&lt;strong&gt;Caf&amp;eacute;&lt;/strong&gt;&amp;nbsp;&amp;amp; housing</title><link href="https://example.org/article"/></entry></feed>"#;
        let articles = parse(bytes, &settings())?;
        let keys = legacy_title_keys(legacy);
        assert_eq!(keys, vec![legacy, "Café & housing"]);
        assert!(keys.contains(&articles[0].title));
        Ok(())
    }

    #[test]
    fn legacy_title_keys_preserve_case_punctuation_and_literal_markup_variants() {
        assert_eq!(
            legacy_title_keys("A &amp; B!"),
            vec!["A &amp; B!", "A & B!"]
        );
        assert_eq!(
            legacy_title_keys("A&#160;&amp; B&#33;"),
            vec!["A&#160;&amp; B&#33;", "A & B!"]
        );
        assert_ne!(legacy_title_keys("Title!"), legacy_title_keys("title"));
        assert!(legacy_title_keys(" \t\u{a0}\n ").is_empty());
        assert_eq!(
            legacy_title_keys("<img src='ignored'>"),
            vec!["<img src='ignored'>"]
        );
    }

    #[test]
    fn parse_preserves_current_ineligible_items_for_baselining() -> Result<()> {
        let mut config = settings();
        config.include_keywords = vec!["does not occur".into()];
        let articles = parse(RSS.as_bytes(), &config)?;
        assert_eq!(articles.len(), 2);
        assert!(articles.iter().all(|article| !eligible(article, &config)));
        Ok(())
    }

    #[test]
    fn malformed_partial_and_unsafe_feeds_reject_the_entire_scan() {
        for bytes in [
            "",
            "<rss><channel>",
            "<rss><channel></rss>",
            "<html></html>",
            "<rss/><rss/>",
            "<!DOCTYPE rss><rss/>",
            "<rss><channel><title>&undefined;</title></channel></rss>",
            "<rss><channel><item><guid>opaque</guid></item></channel></rss>",
            "<rss><channel><item><guid>a</guid><link>javascript:alert(1)</link></item></channel></rss>",
        ] {
            assert!(
                parse(bytes.as_bytes(), &settings()).is_err(),
                "accepted invalid input: {bytes}"
            );
        }
    }

    #[test]
    fn valid_empty_feed_returns_an_empty_scan() -> Result<()> {
        assert!(
            parse(
                b"<rss version=\"2.0\"><channel><title>Empty</title></channel></rss>",
                &settings()
            )?
            .is_empty()
        );
        Ok(())
    }

    #[test]
    fn conflicting_duplicate_guid_rejects_a_partial_scan() {
        let bytes = br#"<rss version="2.0"><channel><item><guid>same</guid><link>https://example.org/first</link></item><item><guid>same</guid><link>https://example.org/second</link></item></channel></rss>"#;
        assert!(parse(bytes, &settings()).is_err());
    }

    #[test]
    fn rss_image_enclosure_is_discovered_and_scripts_are_excluded() -> Result<()> {
        let bytes = br#"<rss version="2.0"><channel><item><guid>media</guid><link>https://example.org/media</link><description><![CDATA[<p>Useful summary</p><script>irrelevant</script><style>irrelevant</style>]]></description><enclosure url="https://example.org/photo.jpeg" type="image/jpeg" length="200"/></item></channel></rss>"#;
        let articles = parse(bytes, &settings())?;
        assert_eq!(articles[0].summary, "Useful summary");
        assert_eq!(
            articles[0].image_urls,
            vec!["https://example.org/photo.jpeg"]
        );
        Ok(())
    }

    #[test]
    fn lazy_and_responsive_feed_images_prefer_larger_candidates() -> Result<()> {
        let bytes = br#"<rss version="2.0"><channel><item><link>https://example.org/posts/one</link><description><![CDATA[<img src="data:image/gif;base64,blank" data-src="/lazy.jpg" srcset="/small.jpg 400w, /large.jpg 1600w" alt="Responsive photo">]]></description></item></channel></rss>"#;
        let articles = parse(bytes, &settings())?;
        assert_eq!(
            articles[0].image_urls,
            vec![
                "https://example.org/large.jpg",
                "https://example.org/lazy.jpg"
            ]
        );
        assert_eq!(articles[0].image_alt.as_deref(), Some("Responsive photo"));
        Ok(())
    }

    #[tokio::test]
    async fn unrequested_not_modified_and_invalid_stored_validators_are_rejected() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(304))
            .mount(&server)
            .await;
        let transport = http::client(&HttpConfig::default())?;
        let config = FeedConfig {
            url: server.uri(),
            ..settings()
        };
        assert!(
            fetch(&transport, &config, &HttpConfig::default(), None)
                .await
                .is_err()
        );
        let validators = FeedValidators {
            etag: Some("secret\ninvalid".into()),
            last_modified: None,
        };
        assert!(
            fetch(
                &transport,
                &config,
                &HttpConfig::default(),
                Some(&validators)
            )
            .await
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn feed_fetch_enforces_body_limit_before_parsing() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(RSS))
            .mount(&server)
            .await;
        let settings = HttpConfig {
            max_feed_bytes: 32,
            ..HttpConfig::default()
        };
        let transport = http::client(&settings)?;
        let config = FeedConfig {
            url: server.uri(),
            ..self::settings()
        };
        assert!(matches!(
            fetch(&transport, &config, &settings, None).await,
            Err(Error::Transport(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn conditional_fetch_sends_both_validators_and_returns_cache_hit() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rss"))
            .and(header("if-none-match", "\"v1\""))
            .and(|request: &wiremock::Request| {
                request
                    .headers
                    .get("if-modified-since")
                    .is_some_and(|value| value == "Thu, 01 Oct 2026 12:00:00 GMT")
            })
            .respond_with(ResponseTemplate::new(304))
            .expect(1)
            .mount(&server)
            .await;
        let config = FeedConfig {
            url: format!("{}/rss", server.uri()),
            ..settings()
        };
        let validators = FeedValidators {
            etag: Some("\"v1\"".into()),
            last_modified: Some("Thu, 01 Oct 2026 12:00:00 GMT".into()),
        };
        let transport = http::client(&HttpConfig::default())?;
        assert!(matches!(
            fetch(
                &transport,
                &config,
                &HttpConfig::default(),
                Some(&validators)
            )
            .await?,
            FeedFetch::NotModified
        ));
        Ok(())
    }

    #[tokio::test]
    async fn modified_fetch_returns_validators_only_after_successful_parse() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/valid"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("ETag", "\"v2\"")
                    .set_body_string(RSS),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/invalid"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("ETag", "\"bad\"")
                    .set_body_string("<rss><channel>"),
            )
            .mount(&server)
            .await;
        let transport = http::client(&HttpConfig::default())?;
        let config = FeedConfig {
            url: format!("{}/valid", server.uri()),
            ..settings()
        };
        match fetch(&transport, &config, &HttpConfig::default(), None).await? {
            FeedFetch::Modified {
                articles,
                validators,
            } => {
                assert_eq!(articles.len(), 2);
                assert_eq!(validators.etag.as_deref(), Some("\"v2\""));
            }
            FeedFetch::NotModified => return Err(Error::Feed("expected modified response".into())),
        }
        let config = FeedConfig {
            url: format!("{}/invalid", server.uri()),
            ..settings()
        };
        assert!(
            fetch(&transport, &config, &HttpConfig::default(), None)
                .await
                .is_err()
        );
        Ok(())
    }
}
