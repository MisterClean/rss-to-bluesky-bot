//! Template rendering that preserves the article URL and UTF-8 facet boundaries.

use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use unicode_segmentation::UnicodeSegmentation;

use crate::config::web_url;
use crate::model::Article;
use crate::{Error, Result};

const MAX_GRAPHEMES: usize = 300;
const MAX_BYTES: usize = 3_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Title,
    Summary,
    FeedTitle,
    Published,
    Link,
}

enum Token<'a> {
    Literal(&'a str),
    Field(Field),
}

/// Validate supported placeholders and exactly one `{link}` placeholder.
pub fn validate_template(template: &str) -> Result<()> {
    if template.len() > MAX_BYTES {
        return Err(Error::Config("post template exceeds 3000 bytes".into()));
    }
    let tokens = tokens(template)?;
    if tokens
        .iter()
        .filter(|token| matches!(token, Token::Field(Field::Link)))
        .count()
        != 1
    {
        return Err(Error::Config(
            "post template must contain exactly one {link}".into(),
        ));
    }
    if template
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(Error::Config(
            "post template contains unsupported control characters".into(),
        ));
    }
    Ok(())
}

/// Render an immutable post record, shortening dynamic fields at grapheme boundaries.
///
/// An article URL too long to fit the protocol limits is an error: URLs are never
/// shortened, and link facet offsets always address the exact UTF-8 bytes rendered.
pub fn render(article: &Article, template: &str) -> Result<Value> {
    validate_template(template)?;
    web_url(&article.url)
        .map_err(|_| Error::Protocol("article link must be a valid HTTP(S) URL".into()))?;
    if !within_limits(&article.url) {
        return Err(Error::Protocol(
            "article link exceeds post text limits".into(),
        ));
    }
    let tokens = tokens(template)?;
    let published = article
        .published_at
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_default();
    let mut fields = [
        ShortText::new(&article.title),
        ShortText::new(&article.summary),
        ShortText::new(&article.feed_title),
        ShortText::new(&published),
    ];
    let (mut text, mut link_start) = assemble(&tokens, &fields, &article.url);
    // Excerpts and source labels yield their budget first, keeping a useful title.
    for index in [1, 2, 0, 3] {
        while !within_limits(&text) && fields[index].shorten() {
            (text, link_start) = assemble(&tokens, &fields, &article.url);
        }
    }
    if !within_limits(&text) {
        return Err(Error::Protocol(
            "template and article link exceed post text limits".into(),
        ));
    }
    let leading_bytes = text.len() - text.trim_start().len();
    let text = text.trim().to_owned();
    link_start = link_start.saturating_sub(leading_bytes);
    Ok(json!({
        "$type": "app.bsky.feed.post",
        "text": text,
        "facets": [{
            "index": {"byteStart": link_start, "byteEnd": link_start + article.url.len()},
            "features": [{"$type": "app.bsky.richtext.facet#link", "uri": article.url}]
        }],
        "createdAt": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
    }))
}

fn tokens(template: &str) -> Result<Vec<Token<'_>>> {
    let mut result = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find(['{', '}']) {
        if start != 0 {
            result.push(Token::Literal(&rest[..start]));
        }
        if rest.as_bytes()[start] == b'}' {
            return Err(Error::Config(
                "post template contains an unmatched brace".into(),
            ));
        }
        let end = rest[start + 1..]
            .find('}')
            .map(|end| end + start + 1)
            .ok_or_else(|| Error::Config("post template contains an unmatched brace".into()))?;
        let field = match &rest[start + 1..end] {
            "title" => Field::Title,
            "summary" => Field::Summary,
            "feed_title" => Field::FeedTitle,
            "published" => Field::Published,
            "link" => Field::Link,
            _ => {
                return Err(Error::Config(
                    "post template contains an unsupported placeholder".into(),
                ));
            }
        };
        result.push(Token::Field(field));
        rest = &rest[end + 1..];
    }
    if !rest.is_empty() {
        result.push(Token::Literal(rest));
    }
    Ok(result)
}

fn within_limits(text: &str) -> bool {
    text.len() <= MAX_BYTES && text.graphemes(true).take(MAX_GRAPHEMES + 1).count() <= MAX_GRAPHEMES
}

struct ShortText<'a> {
    graphemes: Vec<&'a str>,
    shortened: bool,
}

impl<'a> ShortText<'a> {
    fn new(value: &'a str) -> Self {
        let mut bytes = 0;
        let mut graphemes = Vec::new();
        let mut shortened = false;
        for grapheme in value.graphemes(true) {
            if graphemes.len() >= MAX_GRAPHEMES - 1
                || bytes + grapheme.len() > MAX_BYTES - '…'.len_utf8()
            {
                shortened = true;
                break;
            }
            graphemes.push(grapheme);
            bytes += grapheme.len();
        }
        Self {
            graphemes,
            shortened,
        }
    }

    fn append(&self, text: &mut String) {
        for grapheme in &self.graphemes {
            text.push_str(grapheme);
        }
        if self.shortened && !self.graphemes.is_empty() {
            text.push('…');
        }
    }

    fn shorten(&mut self) -> bool {
        if self.graphemes.pop().is_none() {
            return false;
        }
        self.shortened = true;
        true
    }
}

fn assemble(tokens: &[Token<'_>], fields: &[ShortText<'_>; 4], link: &str) -> (String, usize) {
    let mut text = String::new();
    let mut link_start = 0;
    for token in tokens {
        match token {
            Token::Literal(literal) => text.push_str(literal),
            Token::Field(Field::Link) => {
                link_start = text.len();
                text.push_str(link);
            }
            Token::Field(field) => {
                let index = match field {
                    Field::Title => 0,
                    Field::Summary => 1,
                    Field::FeedTitle => 2,
                    Field::Published => 3,
                    Field::Link => continue,
                };
                fields[index].append(&mut text);
            }
        }
    }
    (text, link_start)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn article(title: &str, summary: &str, url: &str) -> Article {
        Article {
            feed_id: "science".into(),
            id: "entry-1".into(),
            aliases: vec!["entry-1".into(), url.into()],
            url: url.into(),
            title: title.into(),
            summary: summary.into(),
            published_at: None,
            image_urls: Vec::new(),
            image_alt: None,
            feed_title: "Science news".into(),
        }
    }

    #[test]
    fn unicode_facets_use_byte_offsets_and_preserve_source() -> Result<()> {
        let source = article(
            "🧪 Café e\u{301}",
            "An excerpt",
            "https://example.org/article",
        );
        let original = source.clone();
        let record = render(&source, "{title}\n\nRead more: {link}")?;
        let text = record["text"]
            .as_str()
            .ok_or_else(|| Error::Protocol("missing text".into()))?;
        let start = record["facets"][0]["index"]["byteStart"]
            .as_u64()
            .unwrap_or_default() as usize;
        let end = record["facets"][0]["index"]["byteEnd"]
            .as_u64()
            .unwrap_or_default() as usize;
        assert_eq!(&text[start..end], source.url);
        assert_eq!(start, "🧪 Café e\u{301}\n\nRead more: ".len());
        assert_eq!(source, original);
        Ok(())
    }

    #[test]
    fn long_titles_and_summaries_shorten_at_grapheme_boundaries() -> Result<()> {
        let source = article(
            &"👩🏽‍🔬e\u{301}".repeat(400),
            &"研究".repeat(400),
            "https://example.org/read",
        );
        let record = render(&source, "{title}\n{summary}\n{link}")?;
        let text = record["text"].as_str().unwrap_or_default();
        assert!(within_limits(text));
        assert!(text.ends_with(&source.url));
        assert!(text.contains('…'));
        Ok(())
    }

    #[test]
    fn repeated_placeholders_do_not_exceed_limits() -> Result<()> {
        let source = article(&"é".repeat(300), "", "https://example.org/read");
        let record = render(&source, "{title} {title} {link}")?;
        assert!(within_limits(record["text"].as_str().unwrap_or_default()));
        Ok(())
    }

    #[test]
    fn impossible_long_url_is_reported_without_truncating_it() {
        let source = article(
            "Title",
            "",
            &format!("https://example.org/{}", "a".repeat(300)),
        );
        assert!(matches!(
            render(&source, "{title} {link}"),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn long_but_valid_url_is_preserved_while_title_yields_space() -> Result<()> {
        let url = format!("https://example.org/{}", "a".repeat(240));
        let source = article(&"A long title ".repeat(100), "", &url);
        let record = render(&source, "{title}\nRead more: {link}")?;
        let text = record["text"].as_str().unwrap_or_default();
        assert!(within_limits(text));
        assert!(text.ends_with(&url));
        assert_eq!(record["facets"][0]["features"][0]["uri"], url);
        Ok(())
    }

    #[test]
    fn supports_all_declared_placeholders_and_requires_one_link() -> Result<()> {
        let source = article("Title", "Summary", "https://example.org/read");
        let record = render(
            &source,
            "{feed_title}: {title}\n{summary}\n{published}\n{link}",
        )?;
        assert!(
            record["text"]
                .as_str()
                .unwrap_or_default()
                .starts_with("Science news: Title\nSummary")
        );
        for template in [
            "{title}",
            "{link}{link}",
            "{other}{link}",
            "{title}{link",
            "}{link}",
        ] {
            assert!(validate_template(template).is_err());
        }
        Ok(())
    }

    #[test]
    fn excessive_utf8_bytes_are_limited_even_with_few_graphemes() -> Result<()> {
        let source = article(
            &format!("a{}", "\u{301}".repeat(2_000)),
            "",
            "https://example.org/read",
        );
        let record = render(&source, "{title} {link}")?;
        assert!(within_limits(record["text"].as_str().unwrap_or_default()));
        assert_eq!(record["facets"][0]["features"][0]["uri"], source.url);
        Ok(())
    }

    #[test]
    fn url_whitespace_cannot_create_facets_past_trimmed_text() {
        let source = article("Title", "", "https://example.org/read ");
        assert!(render(&source, "{title} {link}").is_err());
    }
}
