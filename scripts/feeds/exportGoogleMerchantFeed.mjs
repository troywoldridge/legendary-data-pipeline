// scripts/exportGoogleMerchantFeed.mjs
// Usage:
//   node scripts/exportGoogleMerchantFeed.mjs
//
// Optional env overrides:
//   DATABASE_URL="postgres://..."
//   SITE_URL="https://legendary-collectibles.com"
//   FEED_OUT="./public/google-feed.tsv"
//   FEED_FORMAT="tsv"   // or "csv" (default: tsv)
//
// Notes:
// - URL-based Merchant Center feeds often choke on literal newlines inside fields.
//   This script strips \r/\n from ALL fields (critical fix for "Too few column delimiters").
// - TSV is strongly recommended for URL feeds (commas in descriptions won't matter).

import "dotenv/config";
import pg from "pg";
import fs from "node:fs";
import path from "node:path";

const { Pool } = pg;

const SITE_URL =
  (process.env.SITE_URL ||
    process.env.NEXT_PUBLIC_SITE_URL ||
    "https://legendary-collectibles.com"
  ).replace(/\/+$/, "");

const FEED_FORMAT = String(process.env.FEED_FORMAT || "tsv").toLowerCase(); // tsv | csv
const DELIM = FEED_FORMAT === "csv" ? "," : "\t";

const OUT_PATH =
  process.env.FEED_OUT ||
  (FEED_FORMAT === "csv" ? "./google-merchant-feed.csv" : "./google-merchant-feed.tsv");

// Google headers (deduped + cleaned)
// NOTE: your prior script had "min energy efficiency class" twice — removed the duplicate.
const HEADERS = [
  "id",
  "title",
  "description",
  "availability",
  "availability date",
  "expiration date",
  "link",
  "mobile link",
  "image link",
  "price",
  "sale price",
  "sale price effective date",
  "identifier exists",
  "gtin",
  "mpn",
  "brand",
  "product highlight",
  "product detail",
  "additional image link",
  "condition",
  "adult",
  "color",
  "size",
  "size type",
  "size system",
  "gender",
  "material",
  "pattern",
  "age group",
  "multipack",
  "is bundle",
  "unit pricing measure",
  "unit pricing base measure",
  "energy efficiency class",
  "min energy efficiency class",
  "item group id",
  "sell on google quantity",
];

function moneyUSDFromCents(cents) {
  const v = Number(cents ?? 0) / 100;
  // Google expects currency like "12.34 USD"
  return `${v.toFixed(2)} USD`;
}

function sanitizeField(value) {
  if (value === null || value === undefined) return "";
  let s = String(value);

  // CRITICAL for Merchant Center URL feeds:
  // remove literal newlines and carriage returns (they often cause row-splitting)
  s = s.replace(/\r\n/g, " ").replace(/\r/g, " ").replace(/\n/g, " ");

  // Remove NULL bytes (rare but can break parsers)
  s = s.replace(/\u0000/g, "");

  // Optional: trim insane whitespace
  s = s.replace(/\s\s+/g, " ").trim();

  return s;
}

function escapeForFormat(value) {
  const s = sanitizeField(value);

  if (FEED_FORMAT === "csv") {
    // CSV escaping: wrap in quotes if delimiter/quote present
    if (/[",]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  // TSV: tabs must be removed/replaced, because tab IS the delimiter
  // (commas are fine; no quoting needed in most cases)
  return s.replace(/\t/g, " ");
}

function buildProductLink(slug) {
  // Adjust if your route differs. This matches your previous script.
  return `${SITE_URL}/products/${encodeURIComponent(String(slug || ""))}`;
}

function mapGoogleCondition(_conditionText) {
  // Google "condition" is typically: new / used / refurbished
  // For collectibles, many stores keep "new" and describe NM/LP in title/description.
  return "new";
}

function mapAvailability(status, qty) {
  const q = Number(qty ?? 0);
  const s = String(status ?? "").toLowerCase();

  // Customize if your DB uses different status values.
  if (s !== "active") return "out_of_stock";
  return q > 0 ? "in_stock" : "out_of_stock";
}

function sellOnGoogleQty(status, qty) {
  const s = String(status ?? "").toLowerCase();
  if (s !== "active") return 0;
  const q = Number(qty ?? 0);
  return q > 0 ? q : 0;
}

function highlightFromRow(r) {
  if (r.is_graded) {
    const g = (r.grader || "").toUpperCase();
    const grade = r.grade_x10 ? (Number(r.grade_x10) / 10).toFixed(1) : "";
    return `${g ? g + " " : ""}${grade ? "Grade " + grade : "Graded"} collectible`;
  }
  if (r.sealed) return "Factory sealed product";
  if (String(r.format || "").toLowerCase() === "accessory") return "Collector accessory";
  return "Collector-quality single";
}

function detailFromRow(r) {
  return r.subtitle || "";
}

// --- MTG: parse Scryfall payload json for an image URL ---
function safeJsonParse(maybeJson) {
  if (!maybeJson) return null;
  if (typeof maybeJson === "object") return maybeJson;
  const s = String(maybeJson);
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function extractScryfallImage(payload) {
  const p = safeJsonParse(payload);
  if (!p) return null;

  const iu = p.image_uris;
  if (iu?.large) return iu.large;
  if (iu?.normal) return iu.normal;
  if (iu?.small) return iu.small;

  const faces = Array.isArray(p.card_faces) ? p.card_faces : [];
  for (const f of faces) {
    const fiu = f?.image_uris;
    if (fiu?.large) return fiu.large;
    if (fiu?.normal) return fiu.normal;
    if (fiu?.small) return fiu.small;
  }

  return null;
}

async function detectCardIdColumn(client) {
  const { rows } = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name='products'
  `);

  const cols = new Set(rows.map((r) => r.column_name));

  const candidates = [
    "card_id",
    "tcg_card_id",
    "pokemon_card_id",
    "ygo_card_id",
    "mtg_card_id",
    "scryfall_id",
    "scryfall_card_id",
  ];

  for (const c of candidates) {
    if (cols.has(c)) return c;
  }

  return null;
}

function placeholderImageFor(r) {
  const game = String(r.game || "").toLowerCase();
  if (game === "pokemon") return `${SITE_URL}/images/placeholder-pokemon.jpg`;
  if (game === "yugioh") return `${SITE_URL}/images/placeholder-yugioh.jpg`;
  if (game === "mtg") return `${SITE_URL}/images/placeholder-mtg.jpg`;
  return `${SITE_URL}/images/placeholder.jpg`;
}

function joinRow(obj) {
  return HEADERS.map((h) => escapeForFormat(obj[h] ?? "")).join(DELIM);
}

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("Missing DATABASE_URL in environment.");
    process.exit(1);
  }

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 5,
  });

  const client = await pool.connect();

  try {
    // --- DB debug: THIS will explain 82 vs 237 instantly ---
    const info = await client.query(`
      SELECT current_database() as db,
             current_user as usr,
             inet_server_addr() as host,
             inet_server_port() as port
    `);
    console.log("DB INFO:", info.rows[0]);

    const cnt = await client.query(`SELECT COUNT(*)::int AS n FROM products`);
    console.log("products count in THIS DB:", cnt.rows[0].n);

    const cardIdCol = await detectCardIdColumn(client);
    console.log("Detected products card id column:", cardIdCol || "(none)");

    // check for products.feed_image_url column
    const { rows: prodCols } = await client.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema='public'
        AND table_name='products'
        AND column_name IN ('feed_image_url')
    `);
    const hasFeedImageUrl = prodCols.some((r) => r.column_name === "feed_image_url");

    const selectFeedImage = hasFeedImageUrl
      ? "p.feed_image_url AS feed_image_url,"
      : "NULL::text AS feed_image_url,";

    const joinPokemon = cardIdCol
      ? `LEFT JOIN tcg_cards tcg ON (p.game='pokemon' AND tcg.id = p.${cardIdCol})`
      : `LEFT JOIN tcg_cards tcg ON false`;

    const joinYgo = cardIdCol
      ? `LEFT JOIN ygo_card_images ygoi ON (p.game='yugioh' AND ygoi.card_id = p.${cardIdCol})`
      : `LEFT JOIN ygo_card_images ygoi ON false`;

    const joinMtg = cardIdCol
      ? `LEFT JOIN scryfall_cards_raw scr ON (p.game='mtg' AND scr.id = p.${cardIdCol})`
      : `LEFT JOIN scryfall_cards_raw scr ON false`;

    const sql = `
      SELECT
        p.id,
        p.title,
        p.slug,
        p.game,
        p.format,
        p.sealed,
        p.is_graded,
        p.grader,
        p.grade_x10,
        p.condition,
        p.price_cents,
        p.compare_at_cents,
        p.inventory_type,
        p.quantity,
        p.status,
        p.subtitle,
        p.description,
        ${selectFeedImage}
        tcg.small_image AS pokemon_small_image,
        tcg.large_image AS pokemon_large_image,
        ygoi.image_url AS ygo_image_url,
        scr.payload AS scryfall_payload
      FROM products p
      ${joinPokemon}
      ${joinYgo}
      ${joinMtg}
      ORDER BY p.created_at ASC NULLS LAST, p.title ASC
    `;

    const { rows } = await client.query(sql);

    const lines = [];
    lines.push(HEADERS.map((h) => escapeForFormat(h)).join(DELIM));

    for (const r of rows) {
      const availability = mapAvailability(r.status, r.quantity);
      const qtyForGoogle = sellOnGoogleQty(r.status, r.quantity);

      // price / sale price
      const pc = Number(r.price_cents ?? 0);
      const compare =
        r.compare_at_cents === null ||
        r.compare_at_cents === undefined ||
        r.compare_at_cents === ""
          ? null
          : Number(r.compare_at_cents);

      let priceOut = moneyUSDFromCents(pc);
      let salePriceOut = "";

      if (compare && compare > pc) {
        priceOut = moneyUSDFromCents(compare); // regular
        salePriceOut = moneyUSDFromCents(pc); // discounted
      }

      // image selection priority:
      let imageLink =
        (r.feed_image_url && String(r.feed_image_url).trim()) ||
        (r.pokemon_large_image && String(r.pokemon_large_image).trim()) ||
        (r.pokemon_small_image && String(r.pokemon_small_image).trim()) ||
        (r.ygo_image_url && String(r.ygo_image_url).trim()) ||
        extractScryfallImage(r.scryfall_payload) ||
        placeholderImageFor(r);

      const link = buildProductLink(r.slug);
      const mobileLink = link;

      const row = {
        "id": r.id,
        "title": r.title,
        "description": r.description || "",
        "availability": availability,
        "availability date": "",
        "expiration date": "",
        "link": link,
        "mobile link": mobileLink,
        "image link": imageLink,
        "price": priceOut,
        "sale price": salePriceOut,
        "sale price effective date": "",
        "identifier exists": "false",
        "gtin": "",
        "mpn": "",
        "brand": "Legendary Collectibles",
        "product highlight": highlightFromRow(r),
        "product detail": detailFromRow(r),
        "additional image link": "",
        "condition": mapGoogleCondition(r.condition),
        "adult": "",
        "color": "",
        "size": "",
        "size type": "",
        "size system": "",
        "gender": "",
        "material": "",
        "pattern": "",
        "age group": "",
        "multipack": "",
        "is bundle": r.format === "bundle" || r.sealed ? "true" : "false",
        "unit pricing measure": "",
        "unit pricing base measure": "",
        "energy efficiency class": "",
        "min energy efficiency class": "",
        "item group id": "",
        "sell on google quantity": String(qtyForGoogle),
      };

      lines.push(joinRow(row));
    }

    const outAbs = path.resolve(OUT_PATH);

    // CRLF line endings are safest for many feed processors
    fs.writeFileSync(outAbs, lines.join("\r\n"), "utf-8");

    console.log(`✅ Exported ${rows.length} products`);
    console.log(`✅ Feed format: ${FEED_FORMAT.toUpperCase()} (delimiter: ${FEED_FORMAT === "csv" ? "comma" : "TAB"})`);
    console.log(`✅ File written to: ${outAbs}`);
    console.log(`SITE_URL: ${SITE_URL}`);

    if (!cardIdCol) {
      console.log("⚠️ No products card-id column detected. Pokémon/YGO/MTG joins will be skipped until you add one.");
    }
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error("Export failed:", err);
  process.exit(1);
});
