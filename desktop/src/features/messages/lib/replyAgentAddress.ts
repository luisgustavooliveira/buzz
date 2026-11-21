/**
 * One-shot agent addressing for thread replies.
 *
 * When enabled at build time (`VITE_BUZZ_REPLY_ADDRESS=on`), a thread reply
 * composer whose effective parent message (the explicit reply target, else
 * the thread head) was authored by a known agent opens with that agent
 * pre-addressed as a composer mention chip. The chip is visible and
 * backspace-deletable, participates in the ordinary mention pipeline, and is
 * never promoted into the persistent audience: it applies to that one send.
 */

export type ReplyAddressSeedState = {
  /** Parent event ids already seeded or deliberately settled this visit. */
  consumedParentEventIds: ReadonlySet<string>;
};

export type ReplyAddressSeedInput = {
  enabled: boolean;
  /** Pubkey of the effective parent's author (reply target or thread head). */
  parentAuthorPubkey: string | null;
  parentEventId: string | null;
  /** Whether the client resolves a pubkey as a known agent identity. */
  isAgentPubkey: (pubkey: string) => boolean;
  /** Composer already holds authored (non-seed) content. */
  hasAuthoredContent: boolean;
};

export type ReplyAddressSeedResult = {
  /** Pubkey to pre-address, or null when no seed applies. */
  seedPubkey: string | null;
  /** True when the parent is settled either way — do not seed it again. */
  consumeParent: boolean;
};

const NO_SEED: ReplyAddressSeedResult = {
  seedPubkey: null,
  consumeParent: false,
};

/** Build-time opt-in: only `VITE_BUZZ_REPLY_ADDRESS=on` enables seeding. */
export function replyAgentAddressEnabled(
  env: Record<string, unknown> = import.meta.env ?? {},
): boolean {
  return env.VITE_BUZZ_REPLY_ADDRESS === "on";
}

/**
 * Decide whether a reply composer should pre-address its parent's author.
 *
 * - Disabled or missing parent: no seed, parent stays retryable (agent
 *   identity data may still be loading).
 * - Already-settled parent: never seed twice.
 * - Authored content already present: respect it, settle the parent.
 * - Parent author not (yet) resolved as an agent: stay retryable so a late
 *   roster load can still seed; genuine humans simply never re-trigger.
 * - Otherwise: seed the parent author once and settle it.
 */
export function resolveReplyAddressSeed(
  state: ReplyAddressSeedState,
  input: ReplyAddressSeedInput,
): ReplyAddressSeedResult {
  if (
    !input.enabled ||
    !input.parentEventId ||
    !input.parentAuthorPubkey ||
    state.consumedParentEventIds.has(input.parentEventId)
  ) {
    return NO_SEED;
  }
  if (input.hasAuthoredContent) {
    return { seedPubkey: null, consumeParent: true };
  }
  if (!input.isAgentPubkey(input.parentAuthorPubkey)) {
    return NO_SEED;
  }
  return { seedPubkey: input.parentAuthorPubkey, consumeParent: true };
}
