# GitHub Bridge (optional)

Config-gated GitHub ↔ Buzz bridge. Disabled by default; a relay with no
`BUZZ_GITHUB_*` env vars has no GitHub code paths at all (the webhook route
404s, no client, no worker).

## What it does

- **Inbound**: GitHub `issues` / `issue_comment` webhooks are mirrored into a
  configured Buzz channel. Each issue gets one thread; subsequent comments on
  that issue arrive as replies in the *same* thread, mentioning the agent that
  published the last outbound reply (`last_agent_pubkey`), so the conversation
  is continuous until the issue closes. Comments posted by the bridge itself
  (bot login or known comment id) are suppressed — no echo loops.
- **Outbound (deliberate only)**: nothing is published to GitHub
  automatically. Thread discussion stays private until an agent explicitly
  publishes:
  - `buzz github publish --reply-to <thread-root> --content '...'` — posts the
    reply in-thread AND queues it as a comment on the mirrored issue.
  - `buzz github publish --reply-to <thread-root> --event <msg-id>` — publishes
    an existing thread message.
  - `buzz github open-issue --repo <owner/repo> --title '...' --body '...'
    [--assignee <login>] [--label ...]` — opens an issue in an allowlisted
    repo, credited to the agent, assigned to the named human (fallback: the
    assignee is @mentioned in the body when GitHub rejects the assignment),
    and links the new issue back to the originating thread so replies flow
    both ways.

Outbound actions are durable: the kind:9 event and its `github_outbox` row
commit in one transaction; a worker delivers with backoff (30s·2ⁿ capped at
15min, 6 attempts) to a terminal `failed` state that surfaces as a visible
error reply in the thread. A relay restart never loses a queued action.

## Setup

1. **Create a GitHub App** (org-level). Permissions: Issues → Read & write,
   Metadata → Read. Subscribe to events: `issues`, `issue_comment`. Note the
   App ID and slug. Webhook URL: `https://<relay-host>/integrations/github/webhook`;
   set a webhook secret and mount it at a path the relay can read.
2. **Install the App** on every repository in the routing table.
3. **Configure the relay** (see `.env.example` for the full surface):

   ```dotenv
   BUZZ_GITHUB_ENABLED=true
   BUZZ_GITHUB_APP_ID=123456
   BUZZ_GITHUB_APP_SLUG=snps-buzz-bridge
   BUZZ_GITHUB_PRIVATE_KEY_PATH=/run/secrets/github-app.pem
   BUZZ_GITHUB_WEBHOOK_SECRET_PATH=/run/secrets/github-webhook-secret
   BUZZ_GITHUB_REPOS=[{"name":"SnpsGroup/SnpsGroup.QuantaPki","channel_id":"<uuid>","default_agent_pubkey":"<64-hex>","owner_pubkey":"<64-hex>"}]
   ```

   - `channel_id` — channel that hosts the mirrored threads (e.g.
     `products-and-technologies`).
   - `default_agent_pubkey` — agent mentioned on new issue threads and the
     routing fallback until the first publish names the real responder.
   - `owner_pubkey` — the registered owner of the routed agents. The bridge
     signs its messages with the relay key and attributes them to this owner
     via `buzz:bridge` delegation tags, which buzz-acp verifies against the
     relay's NIP-11 `self` key (see `crates/buzz-acp/README.md`, "Inbound
     Author Gate"). Bots cannot be assigned GitHub issues; the App posts as
     `<slug>[bot]` with the agent credited in the body.

4. **Backfill existing conversations** (optional) so an in-flight issue keeps
   its thread:

   ```sql
   INSERT INTO github_thread_links
       (community_id, repo_full_name, subject_type, subject_number,
        channel_id, root_event_id, last_agent_pubkey)
   VALUES ('<community-uuid>', 'SnpsGroup/SnpsGroup.QuantaPki', 'issue', 104,
           '<channel-uuid>', decode('<thread-root-event-id-hex>', 'hex'),
           decode('<last-publishing-agent-hex>', 'hex'));
   ```

## Security posture

- Webhook: HMAC-SHA256 (`X-Hub-Signature-256`) constant-time verification;
  delivery-GUID dedup in Redis (outage → 503 so GitHub retries, never
  double-processes). Unconfigured bridge or unmapped Host → generic 404.
- Outbound: the client talks only to `api.github.com` (config-overridable for
  tests), 10s timeout, 1 MiB response cap, redirects disabled.
- Agent wake: bridge messages carry `buzz:bridge`/`buzz:bridge-owner`/
  `buzz:bridge-mention` tags signed by the relay key; a regular user cannot
  forge them, and open-issue targets are restricted to the configured
  repository allowlist (anti-exfiltration).
- Untrusted content: GitHub payloads are truncated before logging and capped
  before entering event content.

## Rollback

Set `BUZZ_GITHUB_ENABLED` off (or remove the vars) and restart: the route
404s and the worker stops. Queued outbox rows stay durable if the feature is
re-enabled. The migration is additive; reverting it is not required to
disable the feature.

## Agent instructions (canonical prompt block)

Agents only learn that publication is deliberate if their prompts say so.
Include this block (or a translation) in each agent's system prompt/persona:

> **GITHUB BRIDGE**: issues of allowlisted repos open a thread in the channel
> mentioning you; new comments arrive as replies shaped
> `**<github-user>** commented on #<N>: <body> (<url>)`. The thread is
> PRIVATE — nothing you write in it reaches GitHub by itself. Responding on
> GitHub is a deliberate act:
> `buzz github publish --channel <uuid> --reply-to <thread-root> --content "..."`
> (or `--event <id>` to publish an existing thread message). To open a demand
> for another team: `buzz github open-issue --repo <org/repo> --title "..."
> --body "..." [--assignee <login>]` (repos restricted to the relay allowlist;
> the issue is authored by `snps-buzz-bridge[bot]` with your name credited,
> and replies flow back into this thread addressed to you). The GitHub side
> only sees what you publish — analyze internally, then publish ONE complete,
> consolidated answer; never publish drafts or internal handoffs. After you
> publish, subsequent GitHub comments on that issue route to you. When the
> issue closes, the bridge notes it in the thread and the cycle ends.
