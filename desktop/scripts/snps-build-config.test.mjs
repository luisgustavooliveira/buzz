import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import test from "node:test";

import { productionBuildIdentity } from "./demo-build-config.mjs";
import {
  snpsBuildConfig,
  snpsBuildIdentity,
  snpsIconDir,
  snpsIconSource,
} from "./snps-build-config.mjs";

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
  assert.equal(
    config.tauriConfig.bundle.windows.nsis.installMode,
    "currentUser",
  );
  assert.deepEqual(config.tauriConfig.plugins["deep-link"].desktop.schemes, [
    config.deepLinkScheme,
  ]);
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

test("SNPS package ships its own icons and frontend brand", () => {
  const { bundle } = snpsBuildConfig().tauriConfig;
  assert.ok(bundle.icon.length > 0);
  for (const icon of bundle.icon) {
    assert.ok(icon.startsWith(`${snpsIconDir}/`), icon);
  }
  assert.ok(bundle.icon.includes(`${snpsIconDir}/icon.ico`));
  assert.equal(bundle.windows.nsis.installerIcon, `${snpsIconDir}/icon.ico`);
  assert.equal(snpsBuildIdentity.frontendBrand, "snps");
  assert.ok(
    existsSync(new URL(`../${snpsIconSource}`, import.meta.url)),
    snpsIconSource,
  );
});
