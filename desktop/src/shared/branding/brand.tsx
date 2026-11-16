import type { CSSProperties } from "react";

import snpsMarkUrl from "./snps/snps-mark.png";

/**
 * Build-time distributor branding.
 *
 * `VITE_BUZZ_BRAND` is read once at build time and statically replaced by
 * Vite, so the unbranded build keeps the stock Buzz identity and the branded
 * lanes only swap the logo, the theme label, the sidebar gradient and the
 * fixed accent of the first-party theme. Unknown values fall back to Buzz.
 */
export type BrandId = "buzz" | "snps";

export const BRAND: BrandId =
  import.meta.env.VITE_BUZZ_BRAND === "snps" ? "snps" : "buzz";

/** True when the frontend was built for the SNPS distribution. */
export const isSnpsBrand = BRAND === "snps";

/** Fixed accent the first-party theme uses in the SNPS distribution. */
export const SNPS_ACCENT = "#3888c0";

/** Display name of the first-party theme in the theme picker. */
export const BRAND_THEME_LABEL = isSnpsBrand ? "SNPS" : "Buzz";

/** Square app icon used next to QR codes and in identity dialogs. */
export const BRAND_APP_ICON_SRC = isSnpsBrand
  ? snpsMarkUrl
  : "/app-icon@2x.png";

/** Optional high-density variant of {@link BRAND_APP_ICON_SRC}. */
export const BRAND_APP_ICON_SRCSET = isSnpsBrand
  ? undefined
  : "/app-icon@2x.png 1x, /app-icon@3x.png 2x";

/** Product name shown as alt text for the brand mark. */
export const BRAND_PRODUCT_NAME = isSnpsBrand ? "Buzz SNPS" : "Buzz";

/** Mark the document root so stylesheet overrides can follow the brand. */
export function applyBrandAttribute(
  root: HTMLElement = document.documentElement,
) {
  if (isSnpsBrand) root.setAttribute("data-brand", BRAND);
  else root.removeAttribute("data-brand");
}

/**
 * The brand logo as a static image. Used in place of the Buzz bee (static,
 * flapping and animated) when the build is branded; sized by `className`
 * exactly like the bee components it replaces.
 */
export function BrandMarkImage({
  className,
  ariaLabel,
  style,
}: {
  className?: string;
  ariaLabel?: string;
  style?: CSSProperties;
}) {
  return (
    <img
      alt={ariaLabel ?? ""}
      aria-hidden={ariaLabel ? undefined : true}
      className={["object-contain", className].filter(Boolean).join(" ")}
      draggable={false}
      src={snpsMarkUrl}
      style={style}
    />
  );
}
