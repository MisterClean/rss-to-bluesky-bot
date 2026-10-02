//! Bounded ATProto publisher using current ATrium generated protocol types.
//!
//! Direct Reqwest requests share the source transport while imposing response
//! bounds. ATrium's credential agent ignores session-store write errors and its
//! generated putRecord type cannot express explicit nullable `swapRecord: null`;
//! these operations therefore use a checked transport rather than that agent.

use crate::{
    Error, Result,
    config::Config,
    media,
    model::{Article, Publisher, Receipt, Reconciliation},
};
use atrium_api::{
    com::atproto::{
        repo::{put_record, upload_blob},
        server::{create_session, get_session, refresh_session},
    },
    did_doc::DidDocument,
    types::{
        LimitedU32,
        string::{Did, Tid},
    },
};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use unicode_segmentation::UnicodeSegmentation;

type AtpSession = create_session::Output;

const COLLECTION: &str = "app.bsky.feed.post";
const RESPONSE_LIMIT: usize = 256 * 1024;

/// Explicit private credentials; deliberately has no Debug implementation.
pub struct Credentials {
    pub identifier: String,
    pub password: String,
}

#[derive(Serialize, Deserialize)]
struct SessionDisk {
    version: u8,
    service: String,
    pds: String,
    pending_auth: bool,
    session: Option<AtpSession>,
}

struct Response {
    status: u16,
    retry_after: Option<u64>,
    value: Value,
}
impl Response {
    fn code(&self) -> Option<&str> {
        self.value.get("error").and_then(Value::as_str)
    }
    fn success(&self) -> bool {
        (200..300).contains(&self.status)
    }
    fn error(&self) -> Error {
        if matches!(self.status, 401 | 403) {
            return Error::Account("authentication failed; response was redacted".into());
        }
        // Only protocol-defined categories are reflected in user-visible errors.
        // Arbitrary bodies and server messages can contain credentials or URLs.
        match self.code() {
            Some("InvalidSwap") => {
                Error::Conflict("record already exists or repository changed".into())
            }
            Some(
                "ExpiredToken"
                | "InvalidToken"
                | "AuthenticationRequired"
                | "AccountTakedown"
                | "AccountDeactivated",
            ) => Error::Account("authentication failed; credentials were not logged".into()),
            _ => Error::Http {
                status: self.status,
                retry_after: self.retry_after,
            },
        }
    }
}

/// An authenticated, identity-pinned account publisher.
pub struct Bluesky {
    config: Config,
    client: Client,
    session: AtpSession,
    pds: String,
    did: String,
    auth_valid: bool,
    last_micros: Cell<i64>,
}

impl Bluesky {
    /// Load configured environment credentials and connect without publishing.
    pub async fn connect(
        config: &Config,
        client: Client,
        expected_did: Option<&str>,
    ) -> Result<Self> {
        let identifier = std::env::var(&config.bluesky.identifier_env).map_err(|_| {
            Error::Account("account identifier environment variable is unavailable".into())
        })?;
        let password = std::env::var(&config.bluesky.password_env).unwrap_or_default();
        Self::connect_with_credentials(
            config,
            client,
            expected_did,
            Credentials {
                identifier,
                password,
            },
        )
        .await
    }

    /// Resume a private session or create one, failing closed on uncertain auth.
    pub async fn connect_with_credentials(
        config: &Config,
        client: Client,
        expected_did: Option<&str>,
        credentials: Credentials,
    ) -> Result<Self> {
        if credentials.identifier.trim().is_empty() {
            return Err(Error::Account("account identifier is empty".into()));
        }
        let stored = read_session(&config.state.session)?;
        let (session, pds, resumed) = if let Some(disk) = stored {
            if disk.version != 1 || disk.pending_auth || disk.service != config.bluesky.service {
                return Err(Error::Account("session is incompatible or authentication outcome is unknown; inspect private session state".into()));
            }
            let session = disk
                .session
                .ok_or_else(|| Error::Account("session is incomplete".into()))?;
            validate_session(&session)?;
            check_expected(session.did.as_str(), config, expected_did)?;
            validate_endpoint(&disk.pds)?;
            (session, disk.pds, true)
        } else {
            if credentials.password.is_empty() {
                return Err(Error::Account(
                    "account password environment variable is unavailable".into(),
                ));
            }
            let pending = SessionDisk {
                version: 1,
                service: config.bluesky.service.clone(),
                pds: config.bluesky.service.clone(),
                pending_auth: true,
                session: None,
            };
            write_session(&config.state.session, &pending)?;
            let input = create_session::InputData {
                identifier: credentials.identifier.clone(),
                password: credentials.password,
                allow_takendown: None,
                auth_factor_token: None,
            };
            let response = raw_request(
                &client,
                &config.bluesky.service,
                Method::POST,
                create_session::NSID,
                None,
                None,
                Some(("application/json", serde_json::to_vec(&input)?)),
            )
            .await
            .map_err(|_| {
                Error::Account("login outcome is unknown; inspect private session state".into())
            })?;
            if !response.success() {
                return Err(Error::Account(
                    "login failed; authentication response was redacted".into(),
                ));
            }
            let session: AtpSession = serde_json::from_value(response.value).map_err(|_| {
                Error::Account(
                    "login response is invalid; authentication outcome is unknown".into(),
                )
            })?;
            validate_session(&session)?;
            check_expected(session.did.as_str(), config, expected_did)?;
            let pds = resolve_pds(
                &client,
                &session,
                Duration::from_secs(config.http.timeout_seconds),
            )
            .await?;
            (session, pds, false)
        };
        let did = session.did.as_str().to_owned();
        let mut publisher = Self {
            config: config.clone(),
            client,
            session,
            pds,
            did,
            auth_valid: true,
            last_micros: Cell::new(0),
        };
        publisher.verify_identity(&credentials.identifier).await?;
        // Identity is checked before replacing the pending login marker. A stored
        // session never falls back to password login after an ambiguous failure.
        if !resumed {
            publisher.persist(false)?;
        }
        Ok(publisher)
    }

    fn persist(&self, pending_auth: bool) -> Result<()> {
        write_session(
            &self.config.state.session,
            &SessionDisk {
                version: 1,
                service: self.config.bluesky.service.clone(),
                pds: self.pds.clone(),
                pending_auth,
                session: Some(self.session.clone()),
            },
        )
    }

    async fn verify_identity(&mut self, identifier: &str) -> Result<()> {
        let response = self
            .request(
                Method::GET,
                get_session::NSID,
                None,
                None,
                "application/json",
            )
            .await?;
        if !response.success() {
            return Err(response.error());
        }
        let current: get_session::Output = serde_json::from_value(response.value)
            .map_err(|_| Error::Account("session identity response is invalid".into()))?;
        if current.did.as_str() != self.did {
            self.auth_valid = false;
            return Err(Error::Account("session account identity changed".into()));
        }
        if current.active == Some(false) {
            return Err(Error::Account("account is inactive".into()));
        }
        let current = current.data;
        self.session.handle = current.handle;
        self.session.email = current.email;
        self.session.did_doc = current.did_doc;
        if identifier != self.did
            && identifier != self.session.handle.as_str()
            && self.session.email.as_deref() != Some(identifier)
        {
            let response = raw_request(
                &self.client,
                &self.config.bluesky.service,
                Method::GET,
                "com.atproto.identity.resolveHandle",
                None,
                Some(&[("handle", identifier)]),
                None,
            )
            .await?;
            if !response.success()
                || response.value.get("did").and_then(Value::as_str) != Some(&self.did)
            {
                return Err(Error::Account(
                    "configured identifier does not resolve to the stored account".into(),
                ));
            }
        }
        let resolved = resolve_pds(
            &self.client,
            &self.session,
            Duration::from_secs(self.config.http.timeout_seconds),
        )
        .await?;
        if resolved != self.pds {
            self.pds = resolved;
            let response = self
                .request(
                    Method::GET,
                    get_session::NSID,
                    None,
                    None,
                    "application/json",
                )
                .await?;
            if !response.success()
                || response.value.get("did").and_then(Value::as_str) != Some(&self.did)
            {
                self.auth_valid = false;
                return Err(Error::Account(
                    "authoritative PDS account validation failed".into(),
                ));
            }
        }
        self.persist(false)
    }

    async fn refresh(&mut self) -> Result<()> {
        self.auth_valid = false;
        self.persist(true)?;
        let response = raw_request(
            &self.client,
            &self.pds,
            Method::POST,
            refresh_session::NSID,
            Some(&self.session.refresh_jwt),
            None,
            None,
        )
        .await
        .map_err(|_| {
            Error::Account(
                "session refresh outcome is unknown; inspect private session state".into(),
            )
        })?;
        if !response.success() {
            return Err(Error::Account(
                "session refresh failed; authentication response was redacted".into(),
            ));
        }
        let rotated: refresh_session::Output =
            serde_json::from_value(response.value).map_err(|_| {
                Error::Account("session refresh response is invalid; outcome is unknown".into())
            })?;
        if rotated.did.as_str() != self.did || rotated.active == Some(false) {
            return Err(Error::Account(
                "refreshed session account identity changed or is inactive".into(),
            ));
        }
        let rotated = rotated.data;
        self.session.access_jwt = rotated.access_jwt;
        self.session.refresh_jwt = rotated.refresh_jwt;
        self.session.handle = rotated.handle;
        self.session.did_doc = rotated.did_doc;
        self.pds = resolve_pds(
            &self.client,
            &self.session,
            Duration::from_secs(self.config.http.timeout_seconds),
        )
        .await?;
        // Never use rotated credentials until the durable replacement succeeds.
        self.persist(false)?;
        self.auth_valid = true;
        Ok(())
    }

    async fn request(
        &mut self,
        method: Method,
        nsid: &str,
        query: Option<&[(&str, &str)]>,
        body: Option<Vec<u8>>,
        mime: &str,
    ) -> Result<Response> {
        if !self.auth_valid {
            return Err(Error::Account(
                "authentication requires private session recovery".into(),
            ));
        }
        let response = raw_request(
            &self.client,
            &self.pds,
            method.clone(),
            nsid,
            Some(&self.session.access_jwt),
            query,
            body.clone().map(|bytes| (mime, bytes)),
        )
        .await?;
        if response.code() == Some("ExpiredToken") && matches!(response.status, 400 | 401) {
            self.refresh().await?;
            return raw_request(
                &self.client,
                &self.pds,
                method,
                nsid,
                Some(&self.session.access_jwt),
                query,
                body.map(|bytes| (mime, bytes)),
            )
            .await;
        }
        Ok(response)
    }

    async fn upload(&mut self, bytes: Vec<u8>, mime: &str) -> Result<Value> {
        let expected_size = bytes.len();
        let response = self
            .request(Method::POST, upload_blob::NSID, None, Some(bytes), mime)
            .await?;
        if !response.success() {
            return Err(response.error());
        }
        let output: upload_blob::Output = serde_json::from_value(response.value)
            .map_err(|_| Error::Protocol("upload response is invalid".into()))?;
        let blob = serde_json::to_value(&output.blob)?;
        if blob.get("$type").and_then(Value::as_str) != Some("blob")
            || blob.get("mimeType").and_then(Value::as_str) != Some(mime)
            || blob.get("size").and_then(Value::as_u64) != Some(expected_size as u64)
        {
            return Err(Error::Protocol(
                "uploaded blob metadata does not match prepared bytes".into(),
            ));
        }
        Ok(blob)
    }

    async fn prepare_media(&mut self, rkey: &str, article: &Article) -> Result<Option<Value>> {
        if !self.config.media.enabled {
            return Ok(None);
        }
        let sources =
            media::discover(&self.client, article, &self.config.media, &self.config.http).await;
        let sources = match sources {
            Ok(sources) => sources,
            Err(error) if self.config.media.required => return Err(error),
            Err(_) => article.image_urls.iter().take(32).cloned().collect(),
        };
        let mut images = Vec::new();
        for source in sources {
            if images.len() >= self.config.media.max_images.min(4) {
                break;
            }
            let prepared = match crate::http::get_bytes_with_timeout(
                &self.client,
                &source,
                self.config.media.max_download_bytes,
                Duration::from_secs(self.config.http.timeout_seconds),
            )
            .await
            {
                Ok(bytes) => media::process(bytes, &self.config.media),
                Err(error) => Err(error),
            };
            let prepared = match prepared {
                Ok(prepared) => prepared,
                Err(error) if self.config.media.required => return Err(error),
                Err(_) => continue,
            };
            let blob = match self.upload(prepared.bytes.clone(), &prepared.mime).await {
                Ok(blob) => blob,
                Err(error)
                    if self.config.media.required
                        || matches!(error, Error::Account(_) | Error::Protocol(_)) =>
                {
                    return Err(error);
                }
                Err(_) => continue,
            };
            let alt: String = article
                .image_alt
                .as_deref()
                .unwrap_or(&article.title)
                .graphemes(true)
                .take(2000)
                .collect();
            let image = json!({"image": blob, "alt": alt,
                "aspectRatio": {"width": prepared.width, "height": prepared.height}});
            media::cache(
                &self.config.state.media_dir,
                rkey,
                images.len(),
                &prepared,
                &image,
            )?;
            images.push(image);
        }
        if images.is_empty() {
            if self.config.media.required {
                return Err(Error::Media("required media is unavailable".into()));
            }
            return Ok(None);
        }
        Ok(Some(
            json!({"$type": "app.bsky.embed.images", "images": images}),
        ))
    }
}

impl Publisher for Bluesky {
    fn account_did(&self) -> &str {
        &self.did
    }
    fn allocate_rkey(&self) -> Result<String> {
        let micros = chrono::Utc::now()
            .timestamp_micros()
            .max(self.last_micros.get().saturating_add(1));
        self.last_micros.set(micros);
        let time = chrono::DateTime::from_timestamp_micros(micros)
            .ok_or_else(|| Error::Protocol("TID timestamp is out of range".into()))?;
        let clock = LimitedU32::<1023>::try_from(0)
            .map_err(|_| Error::Protocol("TID clock is invalid".into()))?;
        let key = Tid::from_datetime(clock, time).as_str().to_owned();
        key.parse::<Tid>()
            .map_err(|_| Error::Protocol("TID allocator returned an invalid key".into()))?;
        Ok(key)
    }
    async fn prepare(&mut self, rkey: &str, article: &Article) -> Result<Value> {
        media::directory(&self.config.state.media_dir, rkey)?;
        let mut record = crate::text::render(article, &self.config.posting.template)?;
        if let Some(embed) = self.prepare_media(rkey, article).await? {
            record["embed"] = embed;
        }
        Ok(record)
    }
    async fn reconcile(&mut self, rkey: &str, record: &Value) -> Result<Reconciliation> {
        media::directory(&self.config.state.media_dir, rkey)?;
        let did = self.did.clone();
        let response = self
            .request(
                Method::GET,
                "com.atproto.repo.getRecord",
                Some(&[("repo", &did), ("collection", COLLECTION), ("rkey", rkey)]),
                None,
                "application/json",
            )
            .await?;
        if !response.success() {
            if response.code() == Some("RecordNotFound") && matches!(response.status, 400 | 404) {
                return Ok(Reconciliation::Absent);
            }
            return Err(response.error());
        }
        if response.value.get("value") != Some(record) {
            return Ok(Reconciliation::Conflict);
        }
        Ok(Reconciliation::Matching(parse_receipt(
            response.value,
            &self.did,
            rkey,
        )?))
    }
    async fn publish(&mut self, rkey: &str, record: &Value) -> Result<Receipt> {
        media::directory(&self.config.state.media_dir, rkey)?;
        if let Some(embed) = record.get("embed") {
            if embed.get("$type").and_then(Value::as_str) != Some("app.bsky.embed.images") {
                return Err(Error::Protocol(
                    "frozen record has an unsupported embed".into(),
                ));
            }
            let images = embed
                .get("images")
                .and_then(Value::as_array)
                .filter(|images| !images.is_empty() && images.len() <= 4)
                .ok_or_else(|| Error::Protocol("frozen record image count is invalid".into()))?;
            for (index, image) in images.iter().enumerate() {
                let (cached, bytes) =
                    media::load(&self.config.state.media_dir, rkey, index, image)?;
                let blob = self.upload(bytes, &cached.mime).await?;
                if image.get("image") != Some(&blob) {
                    return Err(Error::Conflict(
                        "reuploaded image differs from frozen blob reference".into(),
                    ));
                }
            }
        }
        // This field must be present and JSON null. Generated Option<Cid> skips
        // None, which permits overwrite; its serializer is covered by a test.
        let body = json!({"repo": self.did, "collection": COLLECTION, "rkey": rkey,
            "record": record, "validate": true, "swapRecord": null});
        let response = self
            .request(
                Method::POST,
                put_record::NSID,
                None,
                Some(serde_json::to_vec(&body)?),
                "application/json",
            )
            .await?;
        if !response.success() {
            return Err(response.error());
        }
        parse_receipt(response.value, &self.did, rkey)
    }
    fn cleanup(&mut self, rkey: &str) -> Result<()> {
        media::cleanup(&self.config.state.media_dir, rkey)
    }
}

fn validate_session(session: &AtpSession) -> Result<()> {
    if session.access_jwt.is_empty()
        || session.refresh_jwt.is_empty()
        || session.active == Some(false)
    {
        return Err(Error::Account("session is empty or inactive".into()));
    }
    Ok(())
}
fn check_expected(did: &str, config: &Config, stored: Option<&str>) -> Result<()> {
    did.parse::<Did>()
        .map_err(|_| Error::Account("account DID is invalid".into()))?;
    for expected in [config.bluesky.expected_did.as_deref(), stored]
        .into_iter()
        .flatten()
    {
        if expected != did {
            return Err(Error::Account(
                "account DID does not match pinned identity".into(),
            ));
        }
    }
    Ok(())
}
fn validate_endpoint(endpoint: &str) -> Result<()> {
    let url =
        url::Url::parse(endpoint).map_err(|_| Error::Account("PDS endpoint is invalid".into()))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Account("PDS endpoint is invalid".into()));
    }
    Ok(())
}
async fn resolve_pds(client: &Client, session: &AtpSession, timeout: Duration) -> Result<String> {
    let document: DidDocument = if let Some(document) = &session.did_doc {
        serde_json::from_value(serde_json::to_value(document)?)
            .map_err(|_| Error::Account("DID document is invalid".into()))?
    } else {
        let did = session.did.as_str();
        let url = if did.starts_with("did:plc:") {
            format!("https://plc.directory/{did}")
        } else if let Some(web) = did.strip_prefix("did:web:") {
            let mut parts = web.split(':');
            let domain = parts
                .next()
                .ok_or_else(|| Error::Account("web DID is invalid".into()))?
                .replace("%3A", ":")
                .replace("%3a", ":");
            let path = parts.collect::<Vec<_>>().join("/");
            if path.is_empty() {
                format!("https://{domain}/.well-known/did.json")
            } else {
                format!("https://{domain}/{path}/did.json")
            }
        } else {
            return Err(Error::Account(
                "unsupported DID method without a DID document".into(),
            ));
        };
        let bytes = crate::http::get_bytes_with_timeout(client, &url, RESPONSE_LIMIT, timeout)
            .await
            .map_err(|_| Error::Account("DID resolution failed".into()))?;
        serde_json::from_slice(&bytes)
            .map_err(|_| Error::Account("DID document is invalid".into()))?
    };
    if document.id != session.did.as_str() {
        return Err(Error::Account(
            "DID document account identity changed".into(),
        ));
    }
    let pds = document
        .get_pds_endpoint()
        .ok_or_else(|| Error::Account("DID document has no authoritative PDS".into()))?;
    validate_endpoint(&pds)?;
    Ok(pds.trim_end_matches('/').to_owned())
}

async fn raw_request(
    client: &Client,
    endpoint: &str,
    method: Method,
    nsid: &str,
    token: Option<&str>,
    query: Option<&[(&str, &str)]>,
    body: Option<(&str, Vec<u8>)>,
) -> Result<Response> {
    validate_endpoint(endpoint)?;
    let url = format!("{}/xrpc/{nsid}", endpoint.trim_end_matches('/'));
    let mime = body.as_ref().map_or("application/json", |(mime, _)| *mime);
    let mut request = client
        .request(method, &url)
        .header(reqwest::header::CONTENT_TYPE, mime);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(query) = query {
        request = request.query(query);
    }
    if let Some((_, body)) = body {
        request = request.body(body);
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| Error::Transport("XRPC transport failed; request details redacted".into()))?;
    if response.url().origin()
        != url::Url::parse(&url)
            .map_err(|_| Error::Protocol("invalid XRPC endpoint".into()))?
            .origin()
    {
        return Err(Error::Protocol(
            "XRPC endpoint redirected to a different origin".into(),
        ));
    }
    let status = response.status().as_u16();
    let retry_after = match crate::http::status_error(&response) {
        Error::Http { retry_after, .. } => retry_after,
        _ => None,
    };
    if response
        .content_length()
        .is_some_and(|length| length > RESPONSE_LIMIT as u64)
    {
        return Err(Error::Protocol("XRPC response exceeds budget".into()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::Transport("XRPC response interrupted; details redacted".into()))?
    {
        if chunk.len() > RESPONSE_LIMIT.saturating_sub(bytes.len()) {
            return Err(Error::Protocol("XRPC response exceeds budget".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    let value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) if !(200..300).contains(&status) => Value::Null,
        Err(_) => {
            return Err(Error::Protocol(
                "XRPC response is invalid JSON; body redacted".into(),
            ));
        }
    };
    Ok(Response {
        status,
        retry_after,
        value,
    })
}

fn parse_receipt(value: Value, did: &str, rkey: &str) -> Result<Receipt> {
    let output: put_record::Output = serde_json::from_value(value)
        .map_err(|_| Error::Protocol("record receipt is invalid".into()))?;
    let expected = format!("at://{did}/{COLLECTION}/{rkey}");
    if output.uri != expected {
        return Err(Error::Protocol(
            "record receipt URI does not match delivery identity".into(),
        ));
    }
    Ok(Receipt {
        uri: output.uri.clone(),
        cid: serde_json::to_value(&output.cid)?
            .as_str()
            .ok_or_else(|| Error::Protocol("record receipt CID is invalid".into()))?
            .to_owned(),
    })
}

fn read_session(path: &Path) -> Result<Option<SessionDisk>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.len() > RESPONSE_LIMIT as u64 {
        return Err(Error::Account("private session file is invalid".into()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Account(
                "session file must have private permissions (0600)".into(),
            ));
        }
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(RESPONSE_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > RESPONSE_LIMIT {
        return Err(Error::Account("session file exceeds budget".into()));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| Error::Account("session file is corrupt; refusing password fallback".into()))
}

fn write_session(path: &Path, session: &SessionDisk) -> Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !parent.exists() {
        fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
    }
    let name = path
        .file_name()
        .ok_or_else(|| Error::Account("session path is invalid".into()))?;
    let temporary: PathBuf = parent.join(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id()
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let bytes = serde_json::to_vec(session)?;
    if bytes.len() > RESPONSE_LIMIT {
        return Err(Error::Account("session exceeds private file budget".into()));
    }
    let mut file = options.open(&temporary)?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BlueskyConfig, MediaConfig, StateConfig};
    use image::ImageEncoder;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path, query_param},
    };
    const DID: &str = "did:plc:abcdefghijklmnopqrstuvwx";
    const KEY: &str = "3m4nz6k6wv222";
    const CID: &str = "bafkreibme22gw2h7y2h7tg2fhqotaqjucnbc24deqo72b6mkl2egezxhvy";

    fn session_json(endpoint: &str, access: &str, refresh: &str, did: &str) -> Value {
        json!({"did":did, "handle":"example.test", "accessJwt":access, "refreshJwt":refresh,
            "active":true,"didDoc":{"id":did,"service":[{"id":"#atproto_pds", "type":"AtprotoPersonalDataServer", "serviceEndpoint":endpoint}]}})
    }
    fn config(dir: &Path, endpoint: &str) -> Config {
        Config {
            state: StateConfig {
                database: dir.join("history.sqlite3"),
                session: dir.join("session.json"),
                media_dir: dir.join("media"),
            },
            bluesky: BlueskyConfig {
                service: endpoint.into(),
                expected_did: Some(DID.into()),
                ..BlueskyConfig::default()
            },
            media: MediaConfig {
                enabled: false,
                ..MediaConfig::default()
            },
            ..Config::default()
        }
    }
    fn publisher(config: Config, endpoint: &str) -> Result<Bluesky> {
        let session = serde_json::from_value(session_json(endpoint, "access", "refresh", DID))?;
        Ok(Bluesky {
            client: crate::http::client(&config.http)?,
            config,
            session,
            pds: endpoint.into(),
            did: DID.into(),
            auth_valid: true,
            last_micros: Cell::new(0),
        })
    }
    fn post() -> Value {
        json!({"$type":COLLECTION, "text":"fixture", "createdAt":"2026-10-02T00:00:00Z", "facets":[]})
    }
    fn receipt() -> Value {
        json!({"uri":format!("at://{DID}/{COLLECTION}/{KEY}"), "cid":CID})
    }
    fn article(image_url: String) -> Article {
        Article {
            feed_id: "test".into(),
            id: "item".into(),
            aliases: vec![],
            url: "https://example.test/article".into(),
            title: "Article photograph".into(),
            summary: String::new(),
            published_at: None,
            image_urls: vec![image_url],
            image_alt: Some("A photographed streetscape".into()),
            feed_title: "Test feed".into(),
        }
    }

    #[tokio::test]
    async fn sends_guarded_explicit_null_create_only_record() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        Mock::given(method("POST")).and(path("/xrpc/com.atproto.repo.putRecord"))
            .and(body_json(json!({"repo":DID,"collection":COLLECTION,"rkey":KEY,"record":post(),"validate":true,"swapRecord":null})))
            .respond_with(ResponseTemplate::new(200).set_body_json(receipt())).expect(1).mount(&server).await;
        let mut client = publisher(config(dir.path(), &server.uri()), &server.uri())?;
        assert_eq!(
            client.publish(KEY, &post()).await?.uri,
            format!("at://{DID}/{COLLECTION}/{KEY}")
        );
        let generated = put_record::InputData {
            collection: COLLECTION
                .parse()
                .map_err(|_| Error::Protocol("test NSID".into()))?,
            repo: DID
                .parse::<Did>()
                .map_err(|_| Error::Protocol("test DID".into()))?
                .into(),
            rkey: KEY
                .parse()
                .map_err(|_| Error::Protocol("test key".into()))?,
            record: serde_json::from_value(post())?,
            swap_commit: None,
            swap_record: None,
            validate: Some(true),
        };
        assert!(serde_json::to_value(generated)?.get("swapRecord").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn reconciles_full_record_and_holds_same_text_different_embed() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let mut remote = receipt();
        remote["value"] = post();
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.repo.getRecord"))
            .and(query_param("repo", DID))
            .respond_with(ResponseTemplate::new(200).set_body_json(remote))
            .mount(&server)
            .await;
        let mut client = publisher(config(dir.path(), &server.uri()), &server.uri())?;
        assert!(matches!(
            client.reconcile(KEY, &post()).await?,
            Reconciliation::Matching(_)
        ));
        let mut changed = post();
        changed["embed"] = json!({"$type":"app.bsky.embed.images","images":[]});
        assert_eq!(
            client.reconcile(KEY, &changed).await?,
            Reconciliation::Conflict
        );
        let mut changed = post();
        changed["createdAt"] = json!("2026-10-02T00:00:01Z");
        assert_eq!(
            client.reconcile(KEY, &changed).await?,
            Reconciliation::Conflict
        );
        Ok(())
    }

    #[tokio::test]
    async fn absence_requires_record_not_found_and_unknown_errors_are_redacted() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        Mock::given(path("/xrpc/com.atproto.repo.getRecord"))
            .and(query_param("rkey", KEY))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error":"RecordNotFound","message":"private token"})),
            )
            .mount(&server)
            .await;
        let mut client = publisher(config(dir.path(), &server.uri()), &server.uri())?;
        assert_eq!(
            client.reconcile(KEY, &post()).await?,
            Reconciliation::Absent
        );
        Mock::given(path("/xrpc/com.atproto.repo.getRecord"))
            .and(query_param("rkey", "3m4nz6k6wv223"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error":"UnknownTokenSecret","message":"private token"})),
            )
            .mount(&server)
            .await;
        let error = client
            .reconcile("3m4nz6k6wv223", &post())
            .await
            .err()
            .ok_or_else(|| Error::Protocol("test expected failure".into()))?;
        assert!(!error.to_string().contains("private"));
        assert!(!error.to_string().contains("UnknownTokenSecret"));
        assert!(matches!(error, Error::Http { status: 404, .. }));
        Ok(())
    }

    #[tokio::test]
    async fn refreshes_session_and_atomically_persists_rotated_tokens() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let config = config(dir.path(), &server.uri());
        let old: AtpSession =
            serde_json::from_value(session_json(&server.uri(), "expired", "refresh", DID))?;
        write_session(
            &config.state.session,
            &SessionDisk {
                version: 1,
                service: server.uri(),
                pds: server.uri(),
                pending_auth: false,
                session: Some(old),
            },
        )?;
        Mock::given(path("/xrpc/com.atproto.server.getSession"))
            .and(header("authorization", "Bearer expired"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(json!({"error":"ExpiredToken","message":"sensitive"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.refreshSession"))
            .and(header("authorization", "Bearer refresh"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(
                &server.uri(),
                "rotated-access",
                "rotated-refresh",
                DID,
            )))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/xrpc/com.atproto.server.getSession"))
            .and(header("authorization", "Bearer rotated-access"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(
                &server.uri(),
                "rotated-access",
                "rotated-refresh",
                DID,
            )))
            .mount(&server)
            .await;
        let client = Bluesky::connect_with_credentials(
            &config,
            crate::http::client(&config.http)?,
            Some(DID),
            Credentials {
                identifier: "example.test".into(),
                password: String::new(),
            },
        )
        .await?;
        assert_eq!(client.account_did(), DID);
        let disk = read_session(&config.state.session)?
            .ok_or_else(|| Error::State("test session absent".into()))?;
        assert!(!disk.pending_auth);
        let rotated = disk
            .session
            .ok_or_else(|| Error::State("test session absent".into()))?;
        assert_eq!(rotated.access_jwt, "rotated-access");
        assert_eq!(rotated.refresh_jwt, "rotated-refresh");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&config.state.session)?.permissions().mode() & 0o777,
                0o600
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn changed_session_did_fails_without_panicking_or_login_fallback() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let config = config(dir.path(), &server.uri());
        let old: AtpSession =
            serde_json::from_value(session_json(&server.uri(), "access", "refresh", DID))?;
        write_session(
            &config.state.session,
            &SessionDisk {
                version: 1,
                service: server.uri(),
                pds: server.uri(),
                pending_auth: false,
                session: Some(old),
            },
        )?;
        Mock::given(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(
                &server.uri(),
                "access",
                "refresh",
                "did:plc:zyxwvutsrqponmlkjihgfedc",
            )))
            .mount(&server)
            .await;
        Mock::given(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let outcome = Bluesky::connect_with_credentials(
            &config,
            crate::http::client(&config.http)?,
            Some(DID),
            Credentials {
                identifier: "example.test".into(),
                password: "private-password".into(),
            },
        )
        .await;
        assert!(matches!(outcome, Err(Error::Account(_))));
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_refresh_is_durable_and_blocks_later_authentication() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let config = config(dir.path(), &server.uri());
        let old: AtpSession =
            serde_json::from_value(session_json(&server.uri(), "expired", "refresh", DID))?;
        write_session(
            &config.state.session,
            &SessionDisk {
                version: 1,
                service: server.uri(),
                pds: server.uri(),
                pending_auth: false,
                session: Some(old),
            },
        )?;
        Mock::given(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error":"ExpiredToken"})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/xrpc/com.atproto.server.refreshSession"))
            .respond_with(
                ResponseTemplate::new(502).set_body_string("SECRET rotated token response"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let outcome = Bluesky::connect_with_credentials(
            &config,
            crate::http::client(&config.http)?,
            Some(DID),
            Credentials {
                identifier: "example.test".into(),
                password: "secret-password".into(),
            },
        )
        .await;
        let error = outcome
            .err()
            .ok_or_else(|| Error::State("test expected failure".into()))?;
        assert!(!error.to_string().contains("SECRET"));
        assert!(
            read_session(&config.state.session)?
                .ok_or_else(|| Error::State("test session absent".into()))?
                .pending_auth
        );
        let second = Bluesky::connect_with_credentials(
            &config,
            crate::http::client(&config.http)?,
            Some(DID),
            Credentials {
                identifier: "example.test".into(),
                password: "secret-password".into(),
            },
        )
        .await;
        assert!(matches!(second, Err(Error::Account(_))));
        Ok(())
    }

    #[tokio::test]
    async fn uploads_exact_mime_aspect_and_reuses_frozen_bytes_for_publication() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let mut config = config(dir.path(), &server.uri());
        config.media = MediaConfig {
            enabled: true,
            required: true,
            max_images: 1,
            discover_from_article: false,
            ..MediaConfig::default()
        };
        let image = image::RgbImage::from_pixel(40, 20, image::Rgb([90, 40, 180]));
        let mut bytes = Vec::new();
        image::codecs::png::PngEncoder::new(&mut bytes)
            .write_image(image.as_raw(), 40, 20, image::ExtendedColorType::Rgb8)
            .map_err(|_| Error::Media("test encoding failed".into()))?;
        let blob = json!({"$type":"blob", "ref":{"$link":CID}, "mimeType":"image/png", "size":bytes.len()});
        Mock::given(path("/source.png"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes.clone()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.uploadBlob"))
            .and(header("content-type", "image/png"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"blob":blob})))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(path("/xrpc/com.atproto.repo.putRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(receipt()))
            .mount(&server)
            .await;
        let mut client = publisher(config, &server.uri())?;
        let record = client
            .prepare(KEY, &article(format!("{}/source.png", server.uri())))
            .await?;
        assert_eq!(
            record["embed"]["images"][0]["aspectRatio"],
            json!({"width":40,"height":20})
        );
        assert_eq!(
            record["embed"]["images"][0]["alt"],
            "A photographed streetscape"
        );
        client.publish(KEY, &record).await?;
        let requests = server
            .received_requests()
            .await
            .ok_or_else(|| Error::State("test requests unavailable".into()))?;
        let uploads: Vec<_> = requests
            .iter()
            .filter(|request| request.url.path().ends_with("uploadBlob"))
            .collect();
        assert!(uploads.iter().all(|request| request.body == bytes));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_reuploaded_blob_change_before_record_write() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let mut client = publisher(config(dir.path(), &server.uri()), &server.uri())?;
        let prepared = media::PreparedImage {
            bytes: vec![1, 2, 3],
            mime: "image/png".into(),
            width: 2,
            height: 1,
        };
        let image = json!({"image":{"$type":"blob","ref":{"$link":CID},"mimeType":"image/png","size":3},"alt":"fixture","aspectRatio":{"width":2,"height":1}});
        media::cache(&client.config.state.media_dir, KEY, 0, &prepared, &image)?;
        let mut record = post();
        record["embed"] = json!({"$type":"app.bsky.embed.images","images":[image]});
        Mock::given(path("/xrpc/com.atproto.repo.uploadBlob")).respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"blob":{"$type":"blob","ref":{"$link":"bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"mimeType":"image/png","size":3}}))).mount(&server).await;
        Mock::given(path("/xrpc/com.atproto.repo.putRecord"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        assert!(matches!(
            client.publish(KEY, &record).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn validates_tid_allocation_and_rejects_unexpected_receipt_uri() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let mut client = publisher(config(dir.path(), &server.uri()), &server.uri())?;
        let first = client.allocate_rkey()?;
        let second = client.allocate_rkey()?;
        assert!(first.parse::<Tid>().is_ok());
        assert!(second.parse::<Tid>().is_ok());
        assert!(first < second);
        Mock::given(path("/xrpc/com.atproto.repo.putRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"uri":format!("at://{DID}/{COLLECTION}/3m4nz6k6wv223"),"cid":CID}),
            ))
            .mount(&server)
            .await;
        assert!(matches!(
            client.publish(KEY, &post()).await,
            Err(Error::Protocol(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_session_never_falls_back_to_password_login() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let config = config(dir.path(), &server.uri());
        fs::write(&config.state.session, b"{invalid token-bearing file")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&config.state.session, fs::Permissions::from_mode(0o600))?;
        }
        Mock::given(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        assert!(matches!(
            Bluesky::connect_with_credentials(
                &config,
                crate::http::client(&config.http)?,
                Some(DID),
                Credentials {
                    identifier: "example.test".into(),
                    password: "secret-password".into()
                }
            )
            .await,
            Err(Error::Account(_))
        ));
        Ok(())
    }
    #[tokio::test]
    async fn initial_login_resolves_pds_and_routes_records_to_authoritative_server() -> Result<()> {
        let service = MockServer::start().await;
        let pds = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let config = config(dir.path(), &service.uri());
        Mock::given(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(
                &pds.uri(),
                "access",
                "refresh",
                DID,
            )))
            .expect(1)
            .mount(&service)
            .await;
        Mock::given(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(
                &pds.uri(),
                "access",
                "refresh",
                DID,
            )))
            .expect(1)
            .mount(&pds)
            .await;
        Mock::given(path("/xrpc/com.atproto.repo.putRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(receipt()))
            .expect(1)
            .mount(&pds)
            .await;
        Mock::given(path("/xrpc/com.atproto.repo.putRecord"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&service)
            .await;
        let mut client = Bluesky::connect_with_credentials(
            &config,
            crate::http::client(&config.http)?,
            Some(DID),
            Credentials {
                identifier: "example.test".into(),
                password: "fixture-password".into(),
            },
        )
        .await?;
        client.publish(KEY, &post()).await?;
        assert_eq!(
            read_session(&config.state.session)?
                .ok_or_else(|| Error::State("test session absent".into()))?
                .pds,
            pds.uri()
        );
        Ok(())
    }

    #[tokio::test]
    async fn config_and_database_expected_did_both_reject_wrong_session_without_network()
    -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let config = config(dir.path(), &server.uri());
        let session: AtpSession =
            serde_json::from_value(session_json(&server.uri(), "access", "refresh", DID))?;
        write_session(
            &config.state.session,
            &SessionDisk {
                version: 1,
                service: server.uri(),
                pds: server.uri(),
                pending_auth: false,
                session: Some(session),
            },
        )?;
        Mock::given(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let wrong_did = "did:plc:zyxwvutsrqponmlkjihgfedc";
        let outcome = Bluesky::connect_with_credentials(
            &config,
            crate::http::client(&config.http)?,
            Some(wrong_did),
            Credentials {
                identifier: "example.test".into(),
                password: String::new(),
            },
        )
        .await;
        assert!(matches!(outcome, Err(Error::Account(_))));
        let mut changed = config.clone();
        changed.bluesky.expected_did = Some(wrong_did.into());
        let outcome = Bluesky::connect_with_credentials(
            &changed,
            crate::http::client(&changed.http)?,
            Some(DID),
            Credentials {
                identifier: "example.test".into(),
                password: String::new(),
            },
        )
        .await;
        assert!(matches!(outcome, Err(Error::Account(_))));
        Ok(())
    }

    #[tokio::test]
    async fn required_media_defers_and_optional_media_falls_back_before_publication() -> Result<()>
    {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        Mock::given(path("/missing.png"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let mut optional = config(dir.path(), &server.uri());
        optional.media = MediaConfig {
            enabled: true,
            required: false,
            discover_from_article: false,
            ..MediaConfig::default()
        };
        let mut client = publisher(optional.clone(), &server.uri())?;
        let item = article(format!("{}/missing.png", server.uri()));
        assert!(client.prepare(KEY, &item).await?.get("embed").is_none());
        optional.media.required = true;
        let mut client = publisher(optional, &server.uri())?;
        assert!(client.prepare(KEY, &item).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn refreshed_did_change_keeps_pending_marker_and_stops_writes() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let mut client = publisher(config(dir.path(), &server.uri()), &server.uri())?;
        client.session.access_jwt = "expired".into();
        client.persist(false)?;
        Mock::given(path("/xrpc/com.atproto.repo.putRecord"))
            .and(header("authorization", "Bearer expired"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error":"ExpiredToken"})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/xrpc/com.atproto.server.refreshSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(
                &server.uri(),
                "new",
                "rotated",
                "did:plc:zyxwvutsrqponmlkjihgfedc",
            )))
            .expect(1)
            .mount(&server)
            .await;
        assert!(matches!(
            client.publish(KEY, &post()).await,
            Err(Error::Account(_))
        ));
        assert!(
            read_session(&client.config.state.session)?
                .ok_or_else(|| Error::State("test session absent".into()))?
                .pending_auth
        );
        assert!(matches!(
            client.publish(KEY, &post()).await,
            Err(Error::Account(_))
        ));
        Ok(())
    }
    #[tokio::test]
    async fn optional_media_never_swallows_upload_authentication_failure() -> Result<()> {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        let mut config = config(dir.path(), &server.uri());
        config.media = MediaConfig {
            enabled: true,
            required: false,
            discover_from_article: false,
            ..MediaConfig::default()
        };
        let image = image::RgbImage::from_pixel(2, 1, image::Rgb([60, 90, 30]));
        let mut bytes = Vec::new();
        image::codecs::png::PngEncoder::new(&mut bytes)
            .write_image(image.as_raw(), 2, 1, image::ExtendedColorType::Rgb8)
            .map_err(|_| Error::Media("test encoding failed".into()))?;
        Mock::given(path("/photo.png"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
            .mount(&server)
            .await;
        Mock::given(path("/xrpc/com.atproto.repo.uploadBlob"))
            .respond_with(ResponseTemplate::new(403).set_body_json(
                json!({"error":"UnknownAuthFailure","message":"private credentials"}),
            ))
            .mount(&server)
            .await;
        let mut client = publisher(config, &server.uri())?;
        assert!(matches!(
            client
                .prepare(KEY, &article(format!("{}/photo.png", server.uri())))
                .await,
            Err(Error::Account(_))
        ));
        Ok(())
    }
    #[tokio::test]
    async fn transient_non_json_xrpc_errors_preserve_retry_after_without_echoing_body() -> Result<()>
    {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir()?;
        Mock::given(path("/xrpc/com.atproto.repo.putRecord"))
            .respond_with(
                ResponseTemplate::new(503)
                    .insert_header("retry-after", "120")
                    .set_body_string("<html>private upstream token</html>"),
            )
            .mount(&server)
            .await;
        let mut client = publisher(config(dir.path(), &server.uri()), &server.uri())?;
        assert!(matches!(
            client.publish(KEY, &post()).await,
            Err(Error::Http {
                status: 503,
                retry_after: Some(120)
            })
        ));
        Ok(())
    }
}
