import * as React from "react";

import {
  replyAgentAddressEnabled,
  resolveReplyAddressSeed,
} from "@/features/messages/lib/replyAgentAddress";
import type { MessageComposerProps } from "./MessageComposer.types";

/**
 * Seeds a one-shot agent-address mention chip into a thread reply composer
 * when the effective parent message was authored by a known agent. The chip
 * is inserted through the same implicit-prefix mechanism as the persistent
 * audience (visible, deletable, excluded from draft snapshots) but never
 * writes to the persistent audience store, so it applies to a single send.
 */
export function useReplyAgentAddressSeed({
  audienceContext,
  editTarget,
  getPlainText,
  isAgentPubkey,
  restoreAddressedAgentMentions,
}: {
  audienceContext: MessageComposerProps["audienceContext"];
  editTarget: MessageComposerProps["editTarget"];
  getPlainText: () => string;
  isAgentPubkey: (pubkey: string) => boolean;
  restoreAddressedAgentMentions: (
    pubkeys?: readonly string[],
    allowedUnpinnedPubkeys?: readonly string[],
  ) => string;
}): void {
  const enabled = replyAgentAddressEnabled();
  const consumedParentEventIdsRef = React.useRef<ReadonlySet<string>>(
    new Set(),
  );

  React.useLayoutEffect(() => {
    if (!enabled || editTarget != null) return;
    if (audienceContext?.type !== "thread") return;
    const parentEventId = audienceContext.parentEventId ?? null;
    const parentAuthorPubkey = audienceContext.parentAuthorPubkey ?? null;
    const result = resolveReplyAddressSeed(
      { consumedParentEventIds: consumedParentEventIdsRef.current },
      {
        enabled,
        parentAuthorPubkey,
        parentEventId,
        isAgentPubkey,
        hasAuthoredContent: getPlainText().trim().length > 0,
      },
    );
    if (result.consumeParent && parentEventId) {
      const consumedParentEventIds = new Set(consumedParentEventIdsRef.current);
      consumedParentEventIds.add(parentEventId);
      consumedParentEventIdsRef.current = consumedParentEventIds;
    }
    if (result.seedPubkey) {
      restoreAddressedAgentMentions([result.seedPubkey], [result.seedPubkey]);
    }
  }, [
    audienceContext,
    editTarget,
    enabled,
    getPlainText,
    isAgentPubkey,
    restoreAddressedAgentMentions,
  ]);
}
