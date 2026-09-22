//! `buzz github` — GitHub bridge commands.
//!
//! The relay's optional GitHub bridge reacts to marker tags on kind:9 stream
//! messages. These commands deliberately publish those tagged messages; the
//! bridge (not the CLI) performs the actual GitHub API calls.

use nostr::{EventBuilder, Tag};

use crate::client::{normalize_write_response, BuzzClient};
use crate::error::CliError;
use crate::validate::{
    parse_uuid, read_file_or_stdin, read_or_stdin, sdk_err, validate_hex64,
};

use super::messages::resolve_thread_ref;

/// Maximum GitHub issue title length (characters).
const MAX_TITLE_CHARS: usize = 256;

/// Maximum GitHub issue body length (characters).
const MAX_BODY_CHARS: usize = 60_000;

/// Maximum GitHub label length (characters).
const MAX_LABEL_CHARS: usize = 50;

/// Maximum GitHub login length (characters): one leading alphanumeric plus up
/// to 38 alphanumerics or hyphens.
const MAX_LOGIN_CHARS: usize = 39;

/// Validate a GitHub `owner/repo` coordinate.
///
/// Both segments must be non-empty and use only `A-Z a-z 0-9 . _ -`
/// (matching `^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`).
pub fn validate_github_repo(repo: &str) -> Result<(), CliError> {
    let mut parts = repo.split('/');
    let ok = match (parts.next(), parts.next(), parts.next()) {
        (Some(owner), Some(name), None) => is_repo_segment(owner) && is_repo_segment(name),
        _ => false,
    };
    if !ok {
        return Err(CliError::Usage(format!(
            "invalid GitHub repository {repo:?}: expected <owner>/<repo> using only A-Z a-z 0-9 . _ -"
        )));
    }
    Ok(())
}

fn is_repo_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Validate a GitHub login (e.g. an issue assignee).
///
/// GitHub logins are 1-39 characters, start with an alphanumeric, and
/// otherwise contain only alphanumerics or hyphens
/// (matching `^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$`).
pub fn validate_github_assignee(login: &str) -> Result<(), CliError> {
    let ok = !login.is_empty()
        && login.len() <= MAX_LOGIN_CHARS
        && login.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
        && login.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    if !ok {
        return Err(CliError::Usage(format!(
            "invalid GitHub login {login:?}: must be 1-{MAX_LOGIN_CHARS} characters, start with a letter or digit, and otherwise contain only letters, digits, or hyphens"
        )));
    }
    Ok(())
}

/// Validate an issue title: non-empty and at most 256 characters.
fn validate_issue_title(title: &str) -> Result<(), CliError> {
    if title.trim().is_empty() {
        return Err(CliError::Usage("issue title must not be empty".into()));
    }
    let chars = title.chars().count();
    if chars > MAX_TITLE_CHARS {
        return Err(CliError::Usage(format!(
            "issue title exceeds {MAX_TITLE_CHARS} characters (got {chars})"
        )));
    }
    Ok(())
}

/// Validate an issue body: non-empty and at most 60,000 characters.
fn validate_issue_body(body: &str) -> Result<(), CliError> {
    if body.trim().is_empty() {
        return Err(CliError::Usage("issue body must not be empty".into()));
    }
    let chars = body.chars().count();
    if chars > MAX_BODY_CHARS {
        return Err(CliError::Usage(format!(
            "issue body exceeds {MAX_BODY_CHARS} characters (got {chars})"
        )));
    }
    Ok(())
}

/// Validate a GitHub label: non-empty and at most 50 characters.
fn validate_github_label(label: &str) -> Result<(), CliError> {
    if label.trim().is_empty() {
        return Err(CliError::Usage("label must not be empty".into()));
    }
    let chars = label.chars().count();
    if chars > MAX_LABEL_CHARS {
        return Err(CliError::Usage(format!(
            "label exceeds {MAX_LABEL_CHARS} characters (got {chars})"
        )));
    }
    Ok(())
}

/// Tags marking a kind:9 message as a GitHub publish request.
///
/// With `source_event`, the bridge publishes the referenced message's content
/// as the GitHub comment body instead of this event's own content.
fn github_publish_tags(source_event: Option<&str>) -> Vec<Vec<String>> {
    let mut tags = vec![vec![
        "buzz:github-publish".to_string(),
        "true".to_string(),
    ]];
    if let Some(event_id) = source_event {
        tags.push(vec![
            "github:source-event".to_string(),
            event_id.to_string(),
        ]);
    }
    tags
}

/// Tags carrying an open-issue request for the GitHub bridge.
fn github_open_issue_tags(
    repo: &str,
    title: &str,
    assignee: Option<&str>,
    labels: &[String],
) -> Vec<Vec<String>> {
    let mut tags = vec![
        vec!["buzz:github-action".to_string(), "open-issue".to_string()],
        vec!["github:repo".to_string(), repo.to_string()],
        vec!["github:title".to_string(), title.to_string()],
    ];
    if let Some(assignee) = assignee {
        tags.push(vec!["github:assignee".to_string(), assignee.to_string()]);
    }
    for label in labels {
        tags.push(vec!["github:label".to_string(), label.clone()]);
    }
    tags
}

/// Attach extra tags to an event builder produced by a buzz-sdk builder.
///
/// The SDK builders own the canonical tag set (`h`, NIP-10 `e`, mentions,
/// media, emoji); bridge marker tags are layered on afterwards so the
/// builders stay free of feature-specific plumbing. Tags must not include an
/// `auth` tag — [`BuzzClient::sign_event`] rejects those.
fn attach_extra_tags(
    builder: EventBuilder,
    extra_tags: &[Vec<String>],
) -> Result<EventBuilder, CliError> {
    let tags = extra_tags
        .iter()
        .map(|tag| {
            Tag::parse(tag)
                .map_err(|e| CliError::Other(format!("invalid bridge tag {tag:?}: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(builder.tags(tags))
}

/// Short note used as the content of a publish request that references an
/// existing thread message instead of carrying new content.
fn publish_note_for_source_event(event_id: &str) -> String {
    format!("Publish request for event {}…", &event_id[..8])
}

/// Publish a thread message to GitHub via the relay bridge.
///
/// Sends a kind:9 reply carrying the `buzz:github-publish` marker tag. With
/// `content`, the reply's own content is the GitHub comment body. With
/// `source_event`, the reply is a short note carrying a
/// `github:source-event` tag, and the bridge publishes the referenced
/// event's content instead. Exactly one of the two must be given.
pub async fn cmd_github_publish(
    client: &BuzzClient,
    channel_id: &str,
    reply_to: &str,
    content: Option<&str>,
    source_event: Option<&str>,
) -> Result<(), CliError> {
    let channel_uuid = parse_uuid(channel_id)?;
    validate_hex64(reply_to)?;

    let (content, source_event) = match (content, source_event) {
        (Some(_), Some(_)) => {
            return Err(CliError::Usage(
                "--content and --event are mutually exclusive".into(),
            ))
        }
        (None, None) => {
            return Err(CliError::Usage(
                "one of --content or --event is required".into(),
            ))
        }
        (Some(content), None) => {
            let content = read_or_stdin(content)?;
            if content.trim().is_empty() {
                return Err(CliError::Usage("--content must not be empty".into()));
            }
            (content, None)
        }
        (None, Some(event_id)) => {
            validate_hex64(event_id)?;
            let event_id = event_id.to_ascii_lowercase();
            (publish_note_for_source_event(&event_id), Some(event_id))
        }
    };

    // `--reply-to` is the immediate parent; the thread root is derived from
    // the parent's NIP-10 tags via the relay, same as `messages send`.
    let thread_ref = resolve_thread_ref(client, reply_to).await?;

    let builder = buzz_sdk::build_message(
        channel_uuid,
        &content,
        Some(&thread_ref),
        &[],
        false,
        &[],
        &[],
    )
    .map_err(sdk_err)?;
    let builder = attach_extra_tags(builder, &github_publish_tags(source_event.as_deref()))?;

    let event = client.sign_event(builder)?;
    let resp = client.submit_event(event).await?;
    println!("{}", normalize_write_response(&resp));
    Ok(())
}

/// Parameters for [`cmd_github_open_issue`].
pub struct OpenIssueParams {
    /// GitHub repository as `<owner>/<repo>`.
    pub repo: String,
    /// Issue title (1-256 characters).
    pub title: String,
    /// Issue body; mutually exclusive with `body_file`.
    pub body: Option<String>,
    /// Path to a file containing the issue body, or `-` for stdin; mutually
    /// exclusive with `body`.
    pub body_file: Option<String>,
    /// GitHub login to assign the issue to.
    pub assignee: Option<String>,
    /// Labels to apply to the issue.
    pub labels: Vec<String>,
    /// Channel UUID to post the request message in.
    pub channel_id: String,
    /// Thread message event ID to reply under; omit for a top-level message.
    pub reply_to: Option<String>,
}

/// Ask the relay's GitHub bridge to open a GitHub issue.
///
/// Sends a kind:9 channel message whose content is the issue body, tagged
/// with `buzz:github-action=open-issue` plus the issue metadata tags.
/// Top-level in `channel_id`, or a thread reply when `reply_to` is given.
pub async fn cmd_github_open_issue(
    client: &BuzzClient,
    p: OpenIssueParams,
) -> Result<(), CliError> {
    validate_github_repo(&p.repo)?;
    validate_issue_title(&p.title)?;
    if let Some(assignee) = p.assignee.as_deref() {
        validate_github_assignee(assignee)?;
    }
    for label in &p.labels {
        validate_github_label(label)?;
    }

    let body = match (&p.body, &p.body_file) {
        (Some(_), Some(_)) => {
            return Err(CliError::Usage(
                "--body and --body-file are mutually exclusive".into(),
            ))
        }
        (None, None) => {
            return Err(CliError::Usage(
                "one of --body or --body-file is required".into(),
            ))
        }
        (Some(body), None) => read_or_stdin(body)?,
        (None, Some(path)) => read_file_or_stdin(path)?,
    };
    validate_issue_body(&body)?;

    let channel_uuid = parse_uuid(&p.channel_id)?;
    let thread_ref = match p.reply_to.as_deref() {
        Some(reply_to) => {
            validate_hex64(reply_to)?;
            Some(resolve_thread_ref(client, reply_to).await?)
        }
        None => None,
    };

    let tags = github_open_issue_tags(&p.repo, &p.title, p.assignee.as_deref(), &p.labels);
    let builder = buzz_sdk::build_message(
        channel_uuid,
        &body,
        thread_ref.as_ref(),
        &[],
        false,
        &[],
        &[],
    )
    .map_err(sdk_err)?;
    let builder = attach_extra_tags(builder, &tags)?;

    let event = client.sign_event(builder)?;
    let resp = client.submit_event(event).await?;
    println!("{}", normalize_write_response(&resp));
    Ok(())
}

/// Dispatch a `buzz github` subcommand.
pub async fn dispatch(cmd: crate::GithubCmd, client: &BuzzClient) -> Result<(), CliError> {
    use crate::GithubCmd;
    match cmd {
        GithubCmd::Publish {
            channel,
            reply_to,
            link,
            content,
            event,
        } => {
            let (channel, reply_to) = match link {
                Some(link) => {
                    let parsed = crate::links::parse_message_link(&link)?;
                    (parsed.channel_id, parsed.message_id)
                }
                None => match (channel, reply_to) {
                    (Some(channel), Some(reply_to)) => (channel, reply_to),
                    _ => {
                        return Err(CliError::Usage(
                            "github publish requires either --link or both --channel and --reply-to"
                                .into(),
                        ))
                    }
                },
            };
            cmd_github_publish(
                client,
                &channel,
                &reply_to,
                content.as_deref(),
                event.as_deref(),
            )
            .await
        }
        GithubCmd::OpenIssue {
            repo,
            title,
            body,
            body_file,
            assignee,
            labels,
            channel,
            reply_to,
        } => {
            cmd_github_open_issue(
                client,
                OpenIssueParams {
                    repo,
                    title,
                    body,
                    body_file,
                    assignee,
                    labels,
                    channel_id: channel,
                    reply_to,
                },
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cmd_github_open_issue, cmd_github_publish, github_open_issue_tags, github_publish_tags,
        publish_note_for_source_event, validate_github_assignee, validate_github_label,
        validate_github_repo, validate_issue_body, validate_issue_title, OpenIssueParams,
    };
    use crate::client::BuzzClient;
    use crate::error::CliError;
    use nostr::Keys;
    use serde_json::json;

    const CHANNEL: &str = "123e4567-e89b-12d3-a456-426614174000";
    const ID_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    // ---- validate_github_repo ----

    #[test]
    fn github_repo_accepts_owner_repo_shapes() {
        for repo in ["org/repo", "block/buzz", "a/b", "my-org/my_repo.v2", "O0/-."] {
            assert!(validate_github_repo(repo).is_ok(), "rejected {repo:?}");
        }
    }

    #[test]
    fn github_repo_rejects_malformed_coordinates() {
        for repo in [
            "",
            "repo",
            "org/",
            "/repo",
            "org/repo/extra",
            "org/re po",
            "org/re:po",
            "org$/repo",
        ] {
            let err = validate_github_repo(repo).unwrap_err();
            assert!(matches!(err, CliError::Usage(_)), "accepted {repo:?}");
        }
    }

    // ---- validate_github_assignee ----

    #[test]
    fn github_assignee_follows_login_rules() {
        for login in ["a", "octocat", "Otto-Cat", "a".repeat(39).as_str()] {
            assert!(validate_github_assignee(login).is_ok(), "rejected {login:?}");
        }
        for login in [
            "",
            "-leading-hyphen",
            "has space",
            "has_underscore",
            "é",
            "a".repeat(40).as_str(),
        ] {
            let err = validate_github_assignee(login).unwrap_err();
            assert!(matches!(err, CliError::Usage(_)), "accepted {login:?}");
        }
    }

    // ---- title / body / label bounds ----

    #[test]
    fn issue_title_bounds_are_enforced() {
        assert!(validate_issue_title("Fix the thing").is_ok());
        assert!(validate_issue_title(&"a".repeat(256)).is_ok());
        assert!(validate_issue_title("").is_err());
        assert!(validate_issue_title("   ").is_err());
        assert!(validate_issue_title(&"a".repeat(257)).is_err());
    }

    #[test]
    fn issue_body_bounds_are_enforced() {
        assert!(validate_issue_body("body").is_ok());
        assert!(validate_issue_body(&"a".repeat(60_000)).is_ok());
        assert!(validate_issue_body("").is_err());
        assert!(validate_issue_body("  \n ").is_err());
        assert!(validate_issue_body(&"a".repeat(60_001)).is_err());
    }

    #[test]
    fn github_label_bounds_are_enforced() {
        assert!(validate_github_label("bug").is_ok());
        assert!(validate_github_label(&"a".repeat(50)).is_ok());
        assert!(validate_github_label("").is_err());
        assert!(validate_github_label("  ").is_err());
        assert!(validate_github_label(&"a".repeat(51)).is_err());
    }

    // ---- tag construction ----

    #[test]
    fn publish_tags_minimal_and_with_source_event() {
        assert_eq!(
            github_publish_tags(None),
            vec![vec!["buzz:github-publish", "true"]]
        );
        assert_eq!(
            github_publish_tags(Some(ID_A)),
            vec![
                vec!["buzz:github-publish", "true"],
                vec!["github:source-event", ID_A],
            ]
        );
    }

    #[test]
    fn open_issue_tags_carry_all_metadata() {
        let tags = github_open_issue_tags(
            "org/repo",
            "Title",
            Some("octocat"),
            &["bug".to_string(), "urgent".to_string()],
        );
        assert_eq!(
            tags,
            vec![
                vec!["buzz:github-action", "open-issue"],
                vec!["github:repo", "org/repo"],
                vec!["github:title", "Title"],
                vec!["github:assignee", "octocat"],
                vec!["github:label", "bug"],
                vec!["github:label", "urgent"],
            ]
        );
        let bare = github_open_issue_tags("org/repo", "Title", None, &[]);
        assert_eq!(bare.len(), 3);
        assert!(!bare.iter().any(|t| t[0] == "github:assignee"));
        assert!(!bare.iter().any(|t| t[0] == "github:label"));
    }

    #[test]
    fn publish_note_names_the_source_event_prefix() {
        assert_eq!(
            publish_note_for_source_event(ID_A),
            "Publish request for event aaaaaaaa…"
        );
    }

    // ---- validation happens before any network call ----

    #[tokio::test]
    async fn publish_requires_exactly_one_of_content_or_event() {
        let client =
            BuzzClient::new("http://127.0.0.1:1".into(), Keys::generate(), None, None).unwrap();

        let both = cmd_github_publish(&client, CHANNEL, ID_A, Some("x"), Some(ID_A))
            .await
            .unwrap_err();
        assert!(matches!(both, CliError::Usage(_)));

        let neither = cmd_github_publish(&client, CHANNEL, ID_A, None, None)
            .await
            .unwrap_err();
        assert!(matches!(neither, CliError::Usage(_)));

        let empty = cmd_github_publish(&client, CHANNEL, ID_A, Some("  "), None)
            .await
            .unwrap_err();
        assert!(matches!(empty, CliError::Usage(_)));

        let bad_source = cmd_github_publish(&client, CHANNEL, ID_A, None, Some("not-hex"))
            .await
            .unwrap_err();
        assert!(matches!(bad_source, CliError::Usage(_)));
    }

    #[tokio::test]
    async fn open_issue_validation_fails_before_network() {
        let client =
            BuzzClient::new("http://127.0.0.1:1".into(), Keys::generate(), None, None).unwrap();
        let base = OpenIssueParams {
            repo: "org/repo".into(),
            title: "Bug".into(),
            body: Some("body".into()),
            body_file: None,
            assignee: None,
            labels: vec![],
            channel_id: CHANNEL.into(),
            reply_to: None,
        };

        for (p, case) in [
            (base_helper(&base, |p| p.repo = "no-slash".into()), "bad repo"),
            (base_helper(&base, |p| p.title = "  ".into()), "empty title"),
            (base_helper(&base, |p| p.body = None), "no body source"),
            (
                base_helper(&base, |p| {
                    p.body = Some("x".into());
                    p.body_file = Some("f".into());
                }),
                "both body sources",
            ),
            (
                base_helper(&base, |p| p.assignee = Some("-bad".into())),
                "bad assignee",
            ),
            (
                base_helper(&base, |p| p.labels = vec![String::new()]),
                "empty label",
            ),
        ] {
            let err = cmd_github_open_issue(&client, p).await.unwrap_err();
            assert!(
                matches!(err, CliError::Usage(_)),
                "{case} must fail with a usage error before any relay round-trip, got {err:?}"
            );
        }
    }

    fn base_helper(base: &OpenIssueParams, mutate: impl Fn(&mut OpenIssueParams)) -> OpenIssueParams {
        let mut p = OpenIssueParams {
            repo: base.repo.clone(),
            title: base.title.clone(),
            body: base.body.clone(),
            body_file: base.body_file.clone(),
            assignee: base.assignee.clone(),
            labels: base.labels.clone(),
            channel_id: base.channel_id.clone(),
            reply_to: base.reply_to.clone(),
        };
        mutate(&mut p);
        p
    }

    // ---- event building through a fake relay ----
    //
    // Same seam as messages.rs: a minimal axum server serves /query (thread
    // parent fetch) and captures the signed event posted to /events.

    use axum::body::Bytes as AxumBytes;
    use axum::extract::State as AxumState;
    use axum::http::{HeaderMap as AxumHeaderMap, StatusCode as AxumStatusCode};
    use axum::routing::post as axum_post;
    use axum::Router as AxumRouter;
    use std::net::SocketAddr as StdSocketAddr;
    use std::sync::Arc as StdArc;
    use std::sync::Mutex as StdMutex;
    use tokio::net::TcpListener as TokioTcpListener;

    type CapturedEvent = StdArc<StdMutex<Option<String>>>;

    /// Fake relay: /query returns `query_body`; /events accepts and captures
    /// the submitted event JSON. Returns (base URL, captured event body).
    async fn fake_github_relay(query_body: String) -> (String, CapturedEvent) {
        let captured: CapturedEvent = StdArc::new(StdMutex::new(None));

        type S = (String, CapturedEvent);
        let state: S = (query_body, captured.clone());

        let app = AxumRouter::new()
            .route(
                "/query",
                axum_post(
                    |AxumState((body, _)): AxumState<S>,
                     _headers: AxumHeaderMap,
                     _req: AxumBytes| async move {
                        (
                            AxumStatusCode::OK,
                            [("content-type", "application/json")],
                            body,
                        )
                    },
                ),
            )
            .route(
                "/events",
                axum_post(
                    |AxumState((_, cap)): AxumState<S>,
                     _headers: AxumHeaderMap,
                     body: AxumBytes| async move {
                        *cap.lock().unwrap() = Some(String::from_utf8_lossy(&body).to_string());
                        (
                            AxumStatusCode::OK,
                            [("content-type", "application/json")],
                            r#"{"event_id":"fake0000","accepted":true,"message":""}"#,
                        )
                    },
                ),
            )
            .with_state(state);

        let listener = TokioTcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: StdSocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), captured)
    }

    /// Parent event with no e-tags — top-level, so root == parent.
    fn parent_event_response() -> String {
        json!([{
            "id": ID_A,
            "pubkey": "c".repeat(64),
            "kind": 9,
            "content": "thread root",
            "created_at": 1_700_000_000_u64,
            "tags": [["h", CHANNEL]],
            "sig": "d".repeat(128),
        }])
        .to_string()
    }

    fn submitted_event(captured: &CapturedEvent) -> serde_json::Value {
        let raw = captured.lock().unwrap();
        let raw = raw.as_ref().expect("event must have been submitted");
        serde_json::from_str(raw).expect("submitted body must be an event JSON")
    }

    fn tag_values(event: &serde_json::Value, name: &str) -> Vec<Vec<String>> {
        event["tags"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t.as_array())
            .filter(|t| t.first().and_then(|v| v.as_str()) == Some(name))
            .map(|t| {
                t.iter()
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn publish_with_content_submits_tagged_reply() {
        let (url, captured) = fake_github_relay(parent_event_response()).await;
        let client = BuzzClient::new(url, Keys::generate(), None, None).unwrap();

        cmd_github_publish(&client, CHANNEL, ID_A, Some("Ship it"), None)
            .await
            .unwrap();

        let event = submitted_event(&captured);
        assert_eq!(event["kind"], 9);
        assert_eq!(event["content"], "Ship it");
        assert_eq!(
            tag_values(&event, "buzz:github-publish"),
            vec![vec!["buzz:github-publish", "true"]]
        );
        assert!(tag_values(&event, "github:source-event").is_empty());
        // Same event shape as `messages send --reply-to`: h tag + reply e tag.
        assert_eq!(tag_values(&event, "h"), vec![vec!["h", CHANNEL]]);
        assert_eq!(
            tag_values(&event, "e"),
            vec![vec!["e", ID_A, "", "reply"]]
        );
    }

    #[tokio::test]
    async fn publish_with_source_event_submits_note_and_reference() {
        let (url, captured) = fake_github_relay(parent_event_response()).await;
        let client = BuzzClient::new(url, Keys::generate(), None, None).unwrap();

        cmd_github_publish(&client, CHANNEL, ID_A, None, Some(ID_A))
            .await
            .unwrap();

        let event = submitted_event(&captured);
        assert_eq!(event["content"], "Publish request for event aaaaaaaa…");
        assert_eq!(
            tag_values(&event, "github:source-event"),
            vec![vec!["github:source-event", ID_A]]
        );
        assert_eq!(
            tag_values(&event, "buzz:github-publish"),
            vec![vec!["buzz:github-publish", "true"]]
        );
    }

    #[tokio::test]
    async fn open_issue_submits_tagged_top_level_message() {
        // No --reply-to: no parent fetch, so /query is never needed.
        let (url, captured) = fake_github_relay("[]".into()).await;
        let client = BuzzClient::new(url, Keys::generate(), None, None).unwrap();

        cmd_github_open_issue(
            &client,
            OpenIssueParams {
                repo: "org/repo".into(),
                title: "Crash on startup".into(),
                body: Some("Steps to reproduce…".into()),
                body_file: None,
                assignee: Some("octocat".into()),
                labels: vec!["bug".into(), "urgent".into()],
                channel_id: CHANNEL.into(),
                reply_to: None,
            },
        )
        .await
        .unwrap();

        let event = submitted_event(&captured);
        assert_eq!(event["kind"], 9);
        assert_eq!(event["content"], "Steps to reproduce…");
        assert_eq!(tag_values(&event, "h"), vec![vec!["h", CHANNEL]]);
        assert!(tag_values(&event, "e").is_empty(), "top-level message");
        assert_eq!(
            tag_values(&event, "buzz:github-action"),
            vec![vec!["buzz:github-action", "open-issue"]]
        );
        assert_eq!(
            tag_values(&event, "github:repo"),
            vec![vec!["github:repo", "org/repo"]]
        );
        assert_eq!(
            tag_values(&event, "github:title"),
            vec![vec!["github:title", "Crash on startup"]]
        );
        assert_eq!(
            tag_values(&event, "github:assignee"),
            vec![vec!["github:assignee", "octocat"]]
        );
        assert_eq!(
            tag_values(&event, "github:label"),
            vec![vec!["github:label", "bug"], vec!["github:label", "urgent"]]
        );
    }

    #[tokio::test]
    async fn open_issue_reply_includes_thread_tags() {
        let (url, captured) = fake_github_relay(parent_event_response()).await;
        let client = BuzzClient::new(url, Keys::generate(), None, None).unwrap();

        cmd_github_open_issue(
            &client,
            OpenIssueParams {
                repo: "org/repo".into(),
                title: "Threaded issue".into(),
                body: Some("body".into()),
                body_file: None,
                assignee: None,
                labels: vec![],
                channel_id: CHANNEL.into(),
                reply_to: Some(ID_A.into()),
            },
        )
        .await
        .unwrap();

        let event = submitted_event(&captured);
        assert_eq!(
            tag_values(&event, "e"),
            vec![vec!["e", ID_A, "", "reply"]]
        );
        assert_eq!(
            tag_values(&event, "buzz:github-action"),
            vec![vec!["buzz:github-action", "open-issue"]]
        );
    }
}
