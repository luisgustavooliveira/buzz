# V1 Email Bridge

The optional email bridge lets relay participants send and receive threaded
email as `first.last@example.com`. Outbound delivery uses an SMTP server
(Postal in the reference deployment); inbound delivery uses Exchange Online
and Microsoft Graph notifications.

V1 is closed to external participants. External addresses are rejected on
outbound and quarantined on inbound, even when they supply valid threading
headers.

## Security contract

- The signer identity determines `From`; the CLI has no `--from` option.
- Each agent has a unique address and Exchange mailbox. Mailbox identity is
  authoritative for BCC routing because BCC is not a visible message header.
- Inbound mail must reference exactly one known, active conversation through
  `In-Reply-To` or `References`.
- Sender and receiving agent must be authorized participants in that
  conversation. External mail requires `DMARC=pass` in the first Exchange
  `Authentication-Results` header. Internal M365 replies require an internal-domain
  sender plus Microsoft's `X-MS-Exchange-Organization-AuthAs: Internal` and a
  matching `X-MS-Exchange-CrossTenant-Id`; missing or invalid proof is quarantined.
- Automatic messages, empty bodies, unknown/ambiguous threads and unauthorized
  senders go to quarantine without waking an agent.
- Email content is untrusted. The mirrored Buzz message explicitly warns the
  agent not to treat it as privileged instructions.
- HTML is not mirrored as markup; Exchange `bodyPreview` is used instead.
- Secrets are read from operator-managed files. Never store them in Buzz
  memories, prompts or versioned configuration.

## Agent command

New conversation:

```bash
buzz email send --channel <channel-uuid> \
  --subject "Status" --to owner@example.com --content "Done"
```

Reply in an email-backed Buzz thread:

```bash
buzz email send --channel <channel-uuid> --reply-to <event-id> \
  --subject "Re: Status" --to owner@example.com --cc agent@example.com \
  --content "Thanks"
```

`--bcc` is repeatable and supported. Acceptance means the durable outbox row
committed; final delivery is reported by the worker.

## Relay configuration

Set `BUZZ_EMAIL_ENABLED=true` and provide:

- `BUZZ_EMAIL_SMTP_HOST`, `BUZZ_EMAIL_SMTP_PORT`;
- `BUZZ_EMAIL_SMTP_USERNAME_PATH`, `BUZZ_EMAIL_SMTP_PASSWORD_PATH`;
- `BUZZ_EMAIL_MESSAGE_ID_DOMAIN`;
- `BUZZ_EMAIL_ROUTES`, a JSON object with `accounts` and `allowed_senders`.

Each account contains `agent_pubkey`, `address`, `display_name`, `mailbox_id`,
`owner_pubkey` and optionally `graph_subscription_id`. Allowed senders contain
`address` and `pubkey`. All addresses are canonicalized to lowercase.

For inbound Graph, also set:

- `BUZZ_EMAIL_GRAPH_ENABLED=true`;
- `BUZZ_EMAIL_GRAPH_TENANT_ID`, `BUZZ_EMAIL_GRAPH_CLIENT_ID`;
- `BUZZ_EMAIL_GRAPH_CLIENT_SECRET_PATH`, `BUZZ_EMAIL_GRAPH_CLIENT_STATE_PATH`.

The webhook is `POST /integrations/email/graph`. The relay validates tenant,
clientState, subscription and mailbox before reading the message. Graph
subscriptions are renewed every 30 minutes with a 60-minute lifetime; renewal
failure is logged and exported as a metric.

## Persistence and delivery

Migration `0046_email_bridge.sql` adds conversations, participants, messages,
outbox and quarantine. The outbound event and its outbox row commit in one
transaction. SMTP delivery is at-least-once with a persistent random Message-ID,
bounded exponential retry and a visible terminal failure note.

Inbound dedup is completion-based: a message is considered delivered only after
its Buzz event commits. A processing error returns 5xx so Graph can retry.

## V2 boundary

The schema already carries stable conversation IDs, relay/external participant
types, To/Cc/Bcc roles, approver identity, parent Message-ID and quarantine
metadata. External invitations and forwards are intentionally disabled until
V2 defines their authorization, anti-spoofing and data-exfiltration controls.
