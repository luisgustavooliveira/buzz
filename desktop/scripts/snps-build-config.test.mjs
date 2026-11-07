import assert from "node:assert/strict";
import test from "node:test";

import { productionBuildIdentity } from "./demo-build-config.mjs";
import { snpsBuildConfig, snpsBuildIdentity } from "./snps-build-config.mjs";

test("SNPS identity is stable and distinct from upstream production", () => {
  assert.deepEqual(snpsBuildConfig(), snpsBuildConfig());
  for (const key of [
    "productName",
    "identifier",
    "appDataIdentity",
    "deepLinkScheme",
    "keyringService",
    "nestName",
    "cliName",
  ]) {
    assert.notEqual(snpsBuildIdentity[key], productionBuildIdentity[key], key);
  }
});

test("SNPS package disables upstream updates and installs per user", () => {
  const config = snpsBuildConfig();
  assert.equal(config.tauriConfig.productName, "Buzz SNPS");
  assert.equal(config.tauriConfig.identifier, "com.snpsgroup.buzz.desktop");
  assert.equal(config.tauriConfig.bundle.createUpdaterArtifacts, false);
  assert.equal(config.tauriConfig.bundle.nsis.installMode, "currentUser");
  assert.deepEqual(
    config.tauriConfig.plugins["deep-link"].desktop.schemes,
    [config.deepLinkScheme],
  );
});

test("SNPS agents and clients land on the corporate relay", () => {
  assert.equal(
    snpsBuildIdentity.relayWsUrl,
    "wss://message-relay.snpsgroup.com",
  );
  assert.equal(
    snpsBuildIdentity.relayHttpUrl,
    "https://message-relay.snpsgroup.com",
  );
});
