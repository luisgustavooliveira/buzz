import { writeFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

export const snpsBuildIdentity = Object.freeze({
  slug: "snps",
  productName: "Buzz SNPS",
  identifier: "com.snpsgroup.buzz.desktop",
  appDataIdentity: "com.snpsgroup.buzz.desktop",
  deepLinkScheme: "buzz-demo-snps",
  keyringService: "buzz-desktop-demo.snps",
  nestName: ".buzz-demo-snps",
  cliName: "buzz-demo-snps",
  relayWsUrl: "wss://message-relay.snpsgroup.com",
  relayHttpUrl: "https://message-relay.snpsgroup.com",
  frontendBrand: "snps",
});

/**
 * SNPS app/installer icons. CI generates them from
 * `desktop/branding/snps/app-icon-source.png` with `tauri icon` into this
 * directory (relative to `src-tauri`), so the upstream `icons/` stay untouched.
 */
export const snpsIconDir = "icons-snps";
export const snpsIconSource = "branding/snps/app-icon-source.png";
const snpsBundleIcons = [
  "32x32.png",
  "128x128.png",
  "128x128@2x.png",
  "icon.icns",
  "icon.ico",
].map((name) => `${snpsIconDir}/${name}`);

export function snpsBuildConfig() {
  return {
    ...snpsBuildIdentity,
    tauriConfig: {
      productName: snpsBuildIdentity.productName,
      identifier: snpsBuildIdentity.identifier,
      plugins: {
        "deep-link": {
          desktop: { schemes: [snpsBuildIdentity.deepLinkScheme] },
        },
      },
      bundle: {
        createUpdaterArtifacts: false,
        icon: snpsBundleIcons,
        windows: {
          nsis: {
            installMode: "currentUser",
            installerIcon: `${snpsIconDir}/icon.ico`,
          },
        },
      },
    },
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const [outputPath] = process.argv.slice(2);
  if (!outputPath) {
    console.error("Usage: snps-build-config.mjs <output-config-path>");
    process.exit(2);
  }
  const config = snpsBuildConfig();
  writeFileSync(outputPath, `${JSON.stringify(config.tauriConfig, null, 2)}\n`);
  console.log(JSON.stringify(config));
}
