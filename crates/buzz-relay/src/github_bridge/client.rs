//! Minimal GitHub REST client for a GitHub App installation.
//!
//! Security posture mirrors `buzz-workflow`'s `call_webhook_impl`: a hard 10s
//! timeout, a 1 MiB response cap read incrementally, redirects disabled, and no
//! proxy — the client only ever talks to the operator-configured
//! `api_base_url` (`https://api.github.com` in production; overridable solely
//! so integration tests can point it at a local mock).
//!
//! Authentication is two-tiered, exactly as GitHub prescribes for Apps:
//! 1. an App JWT (RS256, `iss` = app id, 10-minute lifetime) minted from the
//!    App private key, used only for installation lookup and token minting;
//! 2. per-installation tokens minted on demand and cached until 60s before
//!    expiry, used for the actual API calls.
//!
//! The private key file is read lazily at first use and cached; a signing
//! failure triggers at most one re-read per minute, so a rotated key file is
//! picked up without letting a bad path hammer the filesystem.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use jsonwebtoken::EncodingKey;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Hard timeout for every GitHub HTTP request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum bytes read from any GitHub response body.
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// App JWT lifetime. GitHub rejects JWTs over 10 minutes.
const APP_JWT_LIFETIME_SECS: u64 = 600;
// GitHub rejects App JWTs expiring more than 10 minutes out.
const _: () = assert!(APP_JWT_LIFETIME_SECS <= 600);
/// Installation tokens are used until this long before their expiry.
const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);
/// Minimum spacing between private-key re-reads after a signing error.
const KEY_RELOAD_MIN_INTERVAL: Duration = Duration::from_secs(60);
/// `User-Agent` GitHub requires on every request.
const USER_AGENT: &str = "buzz-relay-github-bridge";

/// Errors from the GitHub client. Typed so callers match on variants — never
/// on parsed strings.
#[derive(Debug)]
pub enum GithubClientError {
    /// The App JWT could not be minted (missing/unreadable/invalid private
    /// key) or GitHub rejected the credentials (401/403).
    Auth(String),
    /// The App is not installed on the repository — non-retryable.
    InstallationMissing {
        /// Repository the installation was looked up for.
        repo: String,
    },
    /// GitHub answered 404 (e.g. the repository or issue is gone) —
    /// non-retryable.
    NotFound(String),
    /// GitHub rate-limited the request (403 rate-limit or 429).
    RateLimited {
        /// Value of GitHub's `Retry-After` header, when present.
        retry_after: Option<Duration>,
    },
    /// Any other non-success HTTP status.
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Response body, capped at 256 bytes for logs/errors.
        body: String,
    },
    /// The request never completed (DNS, connect, timeout, body overrun).
    Transport(String),
}

impl std::fmt::Display for GithubClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GithubClientError::Auth(detail) => write!(f, "github auth failed: {detail}"),
            GithubClientError::InstallationMissing { repo } => {
                write!(f, "github app is not installed on {repo}")
            }
            GithubClientError::NotFound(detail) => write!(f, "github resource not found: {detail}"),
            GithubClientError::RateLimited { retry_after } => match retry_after {
                Some(after) => write!(f, "github rate limited (retry after {after:?})"),
                None => write!(f, "github rate limited"),
            },
            GithubClientError::HttpStatus { status, body } => {
                write!(f, "github http {status}: {body}")
            }
            GithubClientError::Transport(detail) => write!(f, "github transport error: {detail}"),
        }
    }
}

impl std::error::Error for GithubClientError {}

impl GithubClientError {
    /// Whether the outbox worker should reschedule the row after this error.
    ///
    /// `InstallationMissing` and `NotFound` are terminal: the App was removed
    /// from the repo or the repo/issue is gone, and retrying cannot fix either.
    /// Everything else (auth — the key file may be rotated, rate limits, 5xx,
    /// transport) can clear on its own.
    pub fn is_retryable(&self) -> bool {
        match self {
            GithubClientError::Auth(_) => true,
            GithubClientError::InstallationMissing { .. } => false,
            GithubClientError::NotFound(_) => false,
            GithubClientError::RateLimited { .. } => true,
            GithubClientError::HttpStatus { status, .. } => *status >= 500 || *status == 429,
            GithubClientError::Transport(_) => true,
        }
    }
}

/// Map a response status + (already capped) body onto the typed error enum.
/// Pure — unit-tested without any HTTP server.
fn classify_status(status: u16, body: &str, retry_after: Option<Duration>) -> GithubClientError {
    match status {
        401 => GithubClientError::Auth("unauthorized (bad or expired App JWT/token)".into()),
        403 => {
            if body.contains("rate limit") {
                GithubClientError::RateLimited { retry_after }
            } else {
                GithubClientError::Auth(format!("forbidden: {}", truncate_bytes(body, 256)))
            }
        }
        404 => GithubClientError::NotFound(truncate_bytes(body, 256).to_string()),
        429 => GithubClientError::RateLimited { retry_after },
        _ => GithubClientError::HttpStatus {
            status,
            body: truncate_bytes(body, 256).to_string(),
        },
    }
}

/// Byte-boundary-safe truncation for error text (mirrors `truncate_reason` in
/// `api/bridge.rs`).
fn truncate_bytes(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Whether a failed `create_issue` should be retried once without assignees.
///
/// GitHub 422s with a validation error naming the assignee(s) when a login has
/// no repo access — the issue itself is still creatable, so the caller drops
/// the assignees (the body already @-mentions them) and tries again.
fn is_assignee_validation_failure(status: u16, body: &str, had_assignees: bool) -> bool {
    had_assignees && status == 422 && body.contains("assignee")
}

/// Validate an `owner/repo` coordinate before it is interpolated into a URL
/// path. Rejects anything that is not exactly one `/` between two non-empty
/// URL-safe segments (`.`/`..` included), so a corrupted stored repo name
/// cannot escape `/repos/{repo}/...`.
fn validate_repo(repo: &str) -> Result<(), GithubClientError> {
    let valid_segment = |segment: &str| {
        !segment.is_empty()
            && segment != "."
            && segment != ".."
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    let valid = repo.len() <= 140
        && repo.split_once('/').is_some_and(|(owner, name)| {
            valid_segment(owner) && valid_segment(name) && !name.contains('/')
        });
    if valid {
        Ok(())
    } else {
        Err(GithubClientError::Transport(format!(
            "invalid repository coordinate {repo:?}"
        )))
    }
}

/// A completed GitHub response with the bits error classification needs.
struct GithubHttpResponse {
    status: reqwest::StatusCode,
    body: String,
    retry_after: Option<Duration>,
}

impl GithubHttpResponse {
    /// Classify a non-success status into the typed error enum.
    fn error(&self) -> GithubClientError {
        classify_status(self.status.as_u16(), &self.body, self.retry_after)
    }
}

/// RS256 claims for the App JWT (GitHub App auth: `iat`, `exp`, `iss`).
#[derive(serde::Serialize)]
struct AppJwtClaims {
    iat: u64,
    exp: u64,
    iss: String,
}

fn app_jwt_claims(app_id: u64, now_unix: u64) -> AppJwtClaims {
    AppJwtClaims {
        iat: now_unix,
        exp: now_unix + APP_JWT_LIFETIME_SECS,
        iss: app_id.to_string(),
    }
}

struct CachedKey {
    key: Option<EncodingKey>,
    loaded_at: Option<Instant>,
    last_reload_attempt: Option<Instant>,
}

impl CachedKey {
    fn new() -> Self {
        CachedKey {
            key: None,
            loaded_at: None,
            last_reload_attempt: None,
        }
    }
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

impl CachedToken {
    fn usable_at(&self, now: Instant) -> bool {
        now + TOKEN_EXPIRY_MARGIN < self.expires_at
    }
}

/// A shareable GitHub App API client (`Arc` it once per relay).
pub struct GithubClient {
    api_base_url: String,
    app_id: u64,
    private_key_path: PathBuf,
    http: reqwest::Client,
    key: Mutex<CachedKey>,
    /// installation tokens by repo full name.
    tokens: Mutex<HashMap<String, CachedToken>>,
}

impl GithubClient {
    /// Build a client bound to `api_base_url`. Never follows redirects and
    /// never uses a proxy so every byte goes to the configured base URL only.
    pub fn new(api_base_url: &str, app_id: u64, private_key_path: PathBuf) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .unwrap_or_default();
        GithubClient {
            api_base_url: api_base_url.trim_end_matches('/').to_string(),
            app_id,
            private_key_path,
            http,
            key: Mutex::new(CachedKey::new()),
            tokens: Mutex::new(HashMap::new()),
        }
    }

    /// Mint (or reuse) a signed App JWT.
    ///
    /// The key file is read lazily; a signing failure triggers one re-read,
    /// but no more than once per [`KEY_RELOAD_MIN_INTERVAL`] so a persistently
    /// broken key path cannot become an unbounded read loop.
    async fn app_jwt(&self) -> Result<String, GithubClientError> {
        let mut cached = self.key.lock().await;

        let load_key = || -> Result<EncodingKey, GithubClientError> {
            let pem = std::fs::read_to_string(&self.private_key_path).map_err(|e| {
                GithubClientError::Auth(format!(
                    "cannot read app private key {}: {e}",
                    self.private_key_path.display()
                ))
            })?;
            EncodingKey::from_rsa_pem(pem.as_bytes())
                .map_err(|e| GithubClientError::Auth(format!("invalid RSA private key: {e}")))
        };

        if cached.key.is_none() {
            cached.key = Some(load_key()?);
            cached.loaded_at = Some(Instant::now());
        }

        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let claims = app_jwt_claims(self.app_id, now);
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let encode = |key: &EncodingKey| -> Result<String, GithubClientError> {
            jsonwebtoken::encode(&header, &claims, key)
                .map_err(|e| GithubClientError::Auth(format!("jwt encode failed: {e}")))
        };

        if let Some(key) = cached.key.as_ref() {
            if let Ok(jwt) = encode(key) {
                return Ok(jwt);
            }
        }

        // Signing failed against the cached key: re-read the file (bounded to
        // once per minute) in case the App key was rotated on disk.
        let may_reload = cached
            .last_reload_attempt
            .is_none_or(|at| at.elapsed() >= KEY_RELOAD_MIN_INTERVAL);
        if !may_reload {
            return Err(GithubClientError::Auth(
                "jwt encode failed and key re-read is rate-limited to one per minute".into(),
            ));
        }
        cached.last_reload_attempt = Some(Instant::now());
        let fresh = load_key()?;
        let jwt = encode(&fresh)?;
        cached.key = Some(fresh);
        cached.loaded_at = Some(Instant::now());
        Ok(jwt)
    }

    /// Resolve (and cache) an installation token for `repo`.
    async fn installation_token(&self, repo: &str) -> Result<String, GithubClientError> {
        validate_repo(repo)?;
        let now = Instant::now();
        if let Some(cached) = self.tokens.lock().await.get(repo) {
            if cached.usable_at(now) {
                return Ok(cached.token.clone());
            }
        }

        let installation_id = self.lookup_installation(repo).await?;
        let jwt = self.app_jwt().await?;
        let url = format!(
            "{}/app/installations/{installation_id}/access_tokens",
            self.api_base_url
        );
        let response = self
            .send_json(reqwest::Method::POST, &url, Some(jwt.as_str()), &json!({}))
            .await?;
        if !response.status.is_success() {
            return Err(match response.error() {
                GithubClientError::NotFound(_) => GithubClientError::InstallationMissing {
                    repo: repo.to_string(),
                },
                other => other,
            });
        }

        #[derive(Deserialize)]
        struct TokenResponse {
            token: String,
            expires_at: String,
        }
        let parsed: TokenResponse = serde_json::from_str(&response.body)
            .map_err(|e| GithubClientError::Transport(format!("token response parse: {e}")))?;
        let expires_at = chrono::DateTime::parse_from_rfc3339(&parsed.expires_at)
            .map_err(|e| GithubClientError::Transport(format!("token expiry parse: {e}")))?;
        let expires_in = (expires_at.timestamp() - chrono::Utc::now().timestamp()).max(0);
        self.tokens.lock().await.insert(
            repo.to_string(),
            CachedToken {
                token: parsed.token.clone(),
                expires_at: now + Duration::from_secs(expires_in as u64),
            },
        );
        debug!(repo, "github installation token minted");
        Ok(parsed.token)
    }

    /// GET `{api_base}/repos/{repo}/installation` → installation id.
    async fn lookup_installation(&self, repo: &str) -> Result<u64, GithubClientError> {
        let jwt = self.app_jwt().await?;
        let url = format!("{}/repos/{repo}/installation", self.api_base_url);
        let response = self
            .send_json(reqwest::Method::GET, &url, Some(jwt.as_str()), &json!({}))
            .await?;
        if !response.status.is_success() {
            return Err(match response.error() {
                GithubClientError::NotFound(_) => GithubClientError::InstallationMissing {
                    repo: repo.to_string(),
                },
                other => other,
            });
        }
        #[derive(Deserialize)]
        struct InstallationResponse {
            id: u64,
        }
        let parsed: InstallationResponse = serde_json::from_str(&response.body)
            .map_err(|e| GithubClientError::Transport(format!("installation parse: {e}")))?;
        Ok(parsed.id)
    }

    /// POST a comment on an issue; returns GitHub's comment id (used for
    /// webhook echo suppression).
    pub async fn create_issue_comment(
        &self,
        repo: &str,
        issue_number: i32,
        body: &str,
    ) -> Result<i64, GithubClientError> {
        validate_repo(repo)?;
        let token = self.installation_token(repo).await?;
        let url = format!(
            "{}/repos/{repo}/issues/{issue_number}/comments",
            self.api_base_url
        );
        let response = self
            .send_json(
                reqwest::Method::POST,
                &url,
                Some(token.as_str()),
                &json!({ "body": body }),
            )
            .await?;
        if !response.status.is_success() {
            return Err(response.error());
        }
        #[derive(Deserialize)]
        struct CommentResponse {
            id: i64,
        }
        let parsed: CommentResponse = serde_json::from_str(&response.body)
            .map_err(|e| GithubClientError::Transport(format!("comment parse: {e}")))?;
        Ok(parsed.id)
    }

    /// Create an issue. On a 422 that names the assignees (assignee without
    /// repo access), retries once WITHOUT assignees and reports the fallback —
    /// the caller already @-mentions the assignee in the body, so the human is
    /// still notified.
    pub async fn create_issue(
        &self,
        repo: &str,
        title: &str,
        body: &str,
        assignees: Vec<String>,
        labels: Vec<String>,
    ) -> Result<CreateIssueOutcome, GithubClientError> {
        validate_repo(repo)?;
        let token = self.installation_token(repo).await?;
        let url = format!("{}/repos/{repo}/issues", self.api_base_url);

        let mut payload = json!({ "title": title, "body": body });
        if !assignees.is_empty() {
            payload["assignees"] = json!(assignees);
        }
        if !labels.is_empty() {
            payload["labels"] = json!(labels);
        }

        let response = self
            .send_json(reqwest::Method::POST, &url, Some(token.as_str()), &payload)
            .await?;
        if !response.status.is_success() {
            if is_assignee_validation_failure(
                response.status.as_u16(),
                &response.body,
                !assignees.is_empty(),
            ) {
                warn!(
                    repo,
                    "github rejected assignees; retrying issue creation without them"
                );
                let mut fallback = json!({ "title": title, "body": body });
                if !labels.is_empty() {
                    fallback["labels"] = json!(labels);
                }
                let fallback_response = self
                    .send_json(reqwest::Method::POST, &url, Some(token.as_str()), &fallback)
                    .await?;
                if !fallback_response.status.is_success() {
                    return Err(fallback_response.error());
                }
                return parse_issue_response(&fallback_response.body).map(|outcome| {
                    CreateIssueOutcome {
                        assignee_fallback: true,
                        ..outcome
                    }
                });
            }
            return Err(response.error());
        }
        parse_issue_response(&response.body).map(|outcome| CreateIssueOutcome {
            assignee_fallback: false,
            ..outcome
        })
    }

    /// One request: optional bearer (App JWT or installation token), JSON
    /// body, hard response cap read incrementally so a hostile server cannot
    /// OOM us.
    async fn send_json(
        &self,
        method: reqwest::Method,
        url: &str,
        bearer: Option<&str>,
        body: &serde_json::Value,
    ) -> Result<GithubHttpResponse, GithubClientError> {
        let mut request = self
            .http
            .request(method, url)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(body);
        if let Some(token) = bearer {
            request = request.bearer_auth(token);
        }

        let mut response = request
            .send()
            .await
            .map_err(|e| GithubClientError::Transport(e.to_string()))?;
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs);
        let status = response.status();

        let mut body_bytes = Vec::new();
        loop {
            let chunk = response
                .chunk()
                .await
                .map_err(|e| GithubClientError::Transport(format!("reading body: {e}")))?;
            match chunk {
                Some(bytes) => {
                    body_bytes.extend_from_slice(&bytes);
                    if body_bytes.len() > MAX_RESPONSE_BYTES {
                        return Err(GithubClientError::Transport(format!(
                            "response body exceeds {} byte limit",
                            MAX_RESPONSE_BYTES
                        )));
                    }
                }
                None => break,
            }
        }
        let body = String::from_utf8_lossy(&body_bytes).into_owned();
        Ok(GithubHttpResponse {
            status,
            body,
            retry_after,
        })
    }
}

/// Result of a successful issue creation.
#[derive(Debug, Clone)]
pub struct CreateIssueOutcome {
    /// The new issue number.
    pub number: i32,
    /// Browser URL of the new issue.
    pub html_url: String,
    /// True when GitHub rejected the requested assignees and the issue was
    /// created without them (the body still @-mentions them).
    pub assignee_fallback: bool,
}

fn parse_issue_response(body: &str) -> Result<CreateIssueOutcome, GithubClientError> {
    #[derive(Deserialize)]
    struct IssueResponse {
        number: i32,
        html_url: String,
    }
    let parsed: IssueResponse = serde_json::from_str(body)
        .map_err(|e| GithubClientError::Transport(format!("issue parse: {e}")))?;
    Ok(CreateIssueOutcome {
        number: parsed.number,
        html_url: parsed.html_url,
        assignee_fallback: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_jwt_claims_carry_github_shape() {
        let claims = app_jwt_claims(123456, 1_000);
        assert_eq!(claims.iss, "123456");
        assert_eq!(claims.iat, 1_000);
        assert_eq!(claims.exp, 1_000 + APP_JWT_LIFETIME_SECS);
    }

    #[test]
    fn classify_status_maps_auth_notfound_and_ratelimit() {
        assert!(matches!(
            classify_status(401, "{}", None),
            GithubClientError::Auth(_)
        ));
        assert!(matches!(
            classify_status(404, "Not Found", None),
            GithubClientError::NotFound(_)
        ));
        assert!(matches!(
            classify_status(429, "rate limit", Some(Duration::from_secs(30))),
            GithubClientError::RateLimited {
                retry_after: Some(_)
            }
        ));
        // 403 splits on the body: rate-limit language beats auth.
        assert!(matches!(
            classify_status(403, "API rate limit exceeded for installation", None),
            GithubClientError::RateLimited { .. }
        ));
        assert!(matches!(
            classify_status(403, "Resource not accessible by integration", None),
            GithubClientError::Auth(_)
        ));
        assert!(matches!(
            classify_status(500, "boom", None),
            GithubClientError::HttpStatus { status: 500, .. }
        ));
    }

    #[test]
    fn retryability_is_terminal_only_for_installation_and_notfound() {
        assert!(!GithubClientError::InstallationMissing { repo: "o/r".into() }.is_retryable());
        assert!(!GithubClientError::NotFound("gone".into()).is_retryable());
        assert!(GithubClientError::Auth("rotated key".into()).is_retryable());
        assert!(GithubClientError::RateLimited { retry_after: None }.is_retryable());
        assert!(GithubClientError::HttpStatus {
            status: 502,
            body: String::new()
        }
        .is_retryable());
        assert!(!GithubClientError::HttpStatus {
            status: 422,
            body: String::new()
        }
        .is_retryable());
        assert!(GithubClientError::Transport("timeout".into()).is_retryable());
    }

    #[test]
    fn assignee_fallback_triggers_only_on_422_naming_assignees() {
        assert!(is_assignee_validation_failure(
            422,
            r#"{"errors":[{"field":"assignees","code":"invalid"}]}"#,
            true
        ));
        assert!(
            !is_assignee_validation_failure(422, r#"{"message":"Validation Failed"}"#, true),
            "422 without assignee detail must surface, not silently drop fields"
        );
        assert!(!is_assignee_validation_failure(500, "assignee", true));
        assert!(
            !is_assignee_validation_failure(422, "assignee mentioned", false),
            "no assignees sent ⇒ nothing to retry without"
        );
    }

    #[test]
    fn validate_repo_rejects_path_escalation_shapes() {
        for good in ["SnpsGroup/QuantaPki", "a-b_c.d/e_f.g-h"] {
            validate_repo(good).expect(good);
        }
        for bad in [
            "",
            "noslash",
            "two/slashes/here",
            "/leading",
            "trailing/",
            "../etc",
            "owner/repo?query",
            "owner/re%20po",
            &"x".repeat(141),
        ] {
            assert!(validate_repo(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn truncate_bytes_respects_char_boundaries() {
        // "é" is 2 bytes; a cap landing mid-codepoint backs off to the boundary.
        let s = "éééé";
        assert_eq!(truncate_bytes(s, 8), "éééé");
        assert_eq!(truncate_bytes(s, 5), "éé");
        assert_eq!(truncate_bytes("abc", 256), "abc");
    }

    #[tokio::test]
    async fn app_jwt_fails_closed_when_key_file_is_missing() {
        let client = GithubClient::new(
            "https://api.github.com",
            1,
            PathBuf::from("/nonexistent/github-app.pem"),
        );
        assert!(matches!(
            client.app_jwt().await,
            Err(GithubClientError::Auth(_))
        ));
    }
}
