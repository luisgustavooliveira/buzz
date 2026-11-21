import assert from "node:assert/strict";
import test from "node:test";

const address = await import("./replyAgentAddress.ts");

const AGENT = "a".repeat(64);
const HUMAN = "b".repeat(64);
const PARENT = "c".repeat(64);
const OTHER_PARENT = "d".repeat(64);

const agentOnly = (pubkey) => pubkey === AGENT;

test("feature is opt-in and only enabled by VITE_BUZZ_REPLY_ADDRESS=on", () => {
  assert.equal(
    address.replyAgentAddressEnabled({ VITE_BUZZ_REPLY_ADDRESS: "on" }),
    true,
  );
  assert.equal(
    address.replyAgentAddressEnabled({ VITE_BUZZ_REPLY_ADDRESS: "off" }),
    false,
  );
  assert.equal(address.replyAgentAddressEnabled({}), false);
  assert.equal(
    address.replyAgentAddressEnabled({ VITE_BUZZ_REPLY_ADDRESS: undefined }),
    false,
  );
});

test("seeds the parent author once when it is a known agent", () => {
  const state = { consumedParentEventIds: new Set() };
  const result = address.resolveReplyAddressSeed(state, {
    enabled: true,
    parentAuthorPubkey: AGENT,
    parentEventId: PARENT,
    isAgentPubkey: agentOnly,
    hasAuthoredContent: false,
  });
  assert.deepEqual(result, { seedPubkey: AGENT, consumeParent: true });
});

test("never seeds the same parent twice", () => {
  const state = { consumedParentEventIds: new Set([PARENT]) };
  const result = address.resolveReplyAddressSeed(state, {
    enabled: true,
    parentAuthorPubkey: AGENT,
    parentEventId: PARENT,
    isAgentPubkey: agentOnly,
    hasAuthoredContent: false,
  });
  assert.deepEqual(result, { seedPubkey: null, consumeParent: false });
});

test("a different parent seeds independently", () => {
  const state = { consumedParentEventIds: new Set([PARENT]) };
  const result = address.resolveReplyAddressSeed(state, {
    enabled: true,
    parentAuthorPubkey: AGENT,
    parentEventId: OTHER_PARENT,
    isAgentPubkey: agentOnly,
    hasAuthoredContent: false,
  });
  assert.deepEqual(result, { seedPubkey: AGENT, consumeParent: true });
});

test("authored content wins: no seed and the parent is settled", () => {
  const state = { consumedParentEventIds: new Set() };
  const result = address.resolveReplyAddressSeed(state, {
    enabled: true,
    parentAuthorPubkey: AGENT,
    parentEventId: PARENT,
    isAgentPubkey: agentOnly,
    hasAuthoredContent: true,
  });
  assert.deepEqual(result, { seedPubkey: null, consumeParent: true });
});

test("a non-agent parent stays retryable while identity data loads", () => {
  const state = { consumedParentEventIds: new Set() };
  const input = {
    enabled: true,
    parentAuthorPubkey: HUMAN,
    parentEventId: PARENT,
    isAgentPubkey: agentOnly,
    hasAuthoredContent: false,
  };
  const first = address.resolveReplyAddressSeed(state, input);
  assert.deepEqual(first, { seedPubkey: null, consumeParent: false });
  // The same parent can still seed once the roster resolves it as an agent.
  const resolvesLater = address.resolveReplyAddressSeed(state, {
    ...input,
    parentAuthorPubkey: AGENT,
  });
  assert.deepEqual(resolvesLater, { seedPubkey: AGENT, consumeParent: true });
});

test("missing parent or disabled feature never seeds or settles", () => {
  const state = { consumedParentEventIds: new Set() };
  assert.deepEqual(
    address.resolveReplyAddressSeed(state, {
      enabled: false,
      parentAuthorPubkey: AGENT,
      parentEventId: PARENT,
      isAgentPubkey: agentOnly,
      hasAuthoredContent: false,
    }),
    { seedPubkey: null, consumeParent: false },
  );
  assert.deepEqual(
    address.resolveReplyAddressSeed(state, {
      enabled: true,
      parentAuthorPubkey: null,
      parentEventId: PARENT,
      isAgentPubkey: agentOnly,
      hasAuthoredContent: false,
    }),
    { seedPubkey: null, consumeParent: false },
  );
  assert.deepEqual(
    address.resolveReplyAddressSeed(state, {
      enabled: true,
      parentAuthorPubkey: AGENT,
      parentEventId: null,
      isAgentPubkey: agentOnly,
      hasAuthoredContent: false,
    }),
    { seedPubkey: null, consumeParent: false },
  );
});
