#!/usr/bin/env node
import "dotenv/config";
import { request } from "undici";

const ORIGIN = (process.env.REVALUE_ORIGIN || "http://127.0.0.1:3001").replace(
  /\/$/,
  "",
);

/**
 * Strip ANSI escape sequences + any non-printable / non-ASCII characters.
 * Undici rejects header values that contain CTLs or invalid bytes.
 */
function sanitizeHeaderValue(input) {
  const s = String(input ?? "");

  // remove ANSI escape sequences like: \x1b[32m
  const noAnsi = s.replace(/\x1B\[[0-9;]*[mK]/g, "");

  // remove CR/LF
  const noNewlines = noAnsi.replace(/[\r\n]/g, "");

  // keep only visible ASCII (space through ~)
  const ascii = noNewlines.replace(/[^\x20-\x7E]/g, "");

  return ascii.trim();
}

const RAW_TOKEN = process.env.ADMIN_API_TOKEN || "";
const ADMIN_API_TOKEN = sanitizeHeaderValue(RAW_TOKEN);

if (!ADMIN_API_TOKEN) {
  console.error("[cronRevalueAll] Missing/empty ADMIN_API_TOKEN after sanitizing");
  console.error(
    `[cronRevalueAll] rawLen=${String(RAW_TOKEN).length} sanitizedLen=${ADMIN_API_TOKEN.length}`,
  );
  process.exit(1);
}

async function main() {
  const url = `${ORIGIN}/api/dev/collection/revalue`;

  console.log(`[cronRevalueAll] origin: ${ORIGIN}`);
  console.log(`[cronRevalueAll] token lens: raw=${String(RAW_TOKEN).length} sanitized=${ADMIN_API_TOKEN.length}`);
  console.log(`[cronRevalueAll] calling ${url}`);

  const res = await request(url, {
    method: "POST",
    headers: {
      "x-admin-token": ADMIN_API_TOKEN,
      accept: "application/json",
    },
    headersTimeout: 10 * 60 * 1000,
    bodyTimeout: 10 * 60 * 1000,
    maxRedirections: 0,
  });

  const text = await res.body.text();

  let json = null;
  try {
    json = JSON.parse(text);
  } catch {}

  if (res.statusCode < 200 || res.statusCode >= 300) {
    console.error(
      `[cronRevalueAll] failed HTTP ${res.statusCode}\n` +
        (json ? JSON.stringify(json, null, 2) : text),
    );
    process.exit(1);
  }

  console.log(
    `[cronRevalueAll] success HTTP ${res.statusCode}\n` +
      (json ? JSON.stringify(json, null, 2) : text),
  );
}

main().catch((err) => {
  console.error("[cronRevalueAll] fatal:", err?.stack || err?.message || err);
  process.exit(1);
});
