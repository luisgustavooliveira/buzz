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
});

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
        nsis: { installMode: "currentUser" },
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
  writeFileSync(
    outputPath,
    `${JSON.stringify(config.tauriConfig, null, 2)}\n`,
  );
  console.log(JSON.stringify(config));
}
