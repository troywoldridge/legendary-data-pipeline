#!/usr/bin/env node
/**
 * Import PriceCharting daily CSV into pricecharting_prices_raw
 *
 * Usage:
 *   node scripts/pricing/10_import_pricecharting_csv.js --game pokemon --file ./data/pricecharting/pokemon.csv
 *   node scripts/pricing/10_import_pricecharting_csv.js --game yugioh  --file ./data/pricecharting/yugioh.csv --date 2025-12-19
 *   node scripts/pricing/10_import_pricecharting_csv.js --game mtg     --file ./data/pricecharting/mtg.csv
 *
 * Env:
 *   DATABASE_URL=postgres://...
 */

const fs = require("fs");
const crypto = require("crypto");
const { Client } = require("pg");

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (!next || next.startsWith("--")) args[key] = true;
      else {
        args[key] = next;
        i++;
      }
    } else args._.push(a);
  }
  return args;
}

// Minimal CSV parser that handles quoted commas
function parseCsv(text) {
  const rows = [];
  let row = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    const next = text[i + 1];

    if (inQuotes) {
      if (ch === '"' && next === '"') {
        cur += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        cur += ch;
      }
    } else {
      if (ch === '"') inQuotes = true;
      else if (ch === ",") {
        row.push(cur);
        cur = "";
      } else if (ch === "\n") {
        row.push(cur);
        rows.push(row);
        row = [];
        cur = "";
      } else if (ch === "\r") {
        // ignore
      } else cur += ch;
    }
  }
  // final cell
  if (cur.length || row.length) {
    row.push(cur);
    rows.push(row);
  }
  return rows;
}

function centsFromMoney(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;

  // allow "$1,234.56" or "1234.56" or "1234"
  const cleaned = s.replace(/[$,]/g, "");
  if (!/^-?\d+(\.\d+)?$/.test(cleaned)) return null;

  const n = Number(cleaned);
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100);
}

function normalizeHeader(h) {
  return String(h || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "");
}

(async function main() {
  const args = parseArgs(process.argv);
  const game = (args.game || "").toLowerCase();
  const file = args.file;
  const sourceDate = args.date || new Date().toISOString().slice(0, 10);

  if (!process.env.DATABASE_URL) {
    console.error("❌ DATABASE_URL not set");
    process.exit(1);
  }
  if (!file) {
    console.error("❌ Missing --file <path>");
    process.exit(1);
  }
  if (!["pokemon", "yugioh", "mtg"].includes(game)) {
    console.error("❌ --game must be pokemon|yugioh|mtg");
    process.exit(1);
  }

  const buf = fs.readFileSync(file);
  const sha = crypto.createHash("sha256").update(buf).digest("hex");
  const text = buf.toString("utf8");

  const rows = parseCsv(text);
  if (rows.length < 2) {
    console.error("❌ CSV has no data");
    process.exit(1);
  }

  const headers = rows[0].map(normalizeHeader);
  const idx = {};
  headers.forEach((h, i) => (idx[h] = i));

  // These are the headers your earlier CSV format used (from our previous work):
  // id, product-name, console-name, release-date, loose-price, cib-price, new-price, graded-price, etc.
  // We’ll support multiple variants by trying common header names.
  function getCell(r, ...names) {
    for (const n of names) {
      const k = normalizeHeader(n);
      if (idx[k] != null) return r[idx[k]] ?? "";
    }
    return "";
  }

  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();

  // record run
  const runRes = await client.query(
    `INSERT INTO public.pricecharting_import_runs (game, file_name, file_sha256, meta)
     VALUES ($1,$2,$3,$4)
     RETURNING id`,
    [game, file, sha, { source_date: sourceDate }]
  );
  const runId = runRes.rows[0].id;

  const upsertSql = `
    INSERT INTO public.pricecharting_prices_raw (
      game, pricecharting_id, product_name, console_name,
      loose_price_cents, cib_price_cents, new_price_cents, graded_price_cents,
      box_only_price_cents, manual_only_price_cents,
      bgs_10_price_cents, cgc_10_price_cents, psa_10_price_cents,
      release_date, source_date, raw, updated_at
    )
    VALUES (
      $1,$2,$3,$4,
      $5,$6,$7,$8,
      $9,$10,
      $11,$12,$13,
      $14,$15,$16, now()
    )
    ON CONFLICT (game, pricecharting_id, source_date)
    DO UPDATE SET
      product_name = EXCLUDED.product_name,
      console_name = EXCLUDED.console_name,
      loose_price_cents = EXCLUDED.loose_price_cents,
      cib_price_cents = EXCLUDED.cib_price_cents,
      new_price_cents = EXCLUDED.new_price_cents,
      graded_price_cents = EXCLUDED.graded_price_cents,
      box_only_price_cents = EXCLUDED.box_only_price_cents,
      manual_only_price_cents = EXCLUDED.manual_only_price_cents,
      bgs_10_price_cents = EXCLUDED.bgs_10_price_cents,
      cgc_10_price_cents = EXCLUDED.cgc_10_price_cents,
      psa_10_price_cents = EXCLUDED.psa_10_price_cents,
      release_date = EXCLUDED.release_date,
      raw = EXCLUDED.raw,
      updated_at = now()
  `;

  let inserted = 0;
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r || r.length === 0) continue;

    const pricecharting_id =
      getCell(r, "id", "pricecharting_id", "product_id").trim();
    const product_name =
      getCell(r, "product-name", "product_name", "name").trim();
    if (!pricecharting_id || !product_name) continue;

    const console_name = getCell(r, "console-name", "console_name").trim() || null;
    const release_date = (getCell(r, "release-date", "release_date") || "").trim() || null;

    // prices
    const loose = centsFromMoney(getCell(r, "loose-price", "loose_price", "loose_price_cents"));
    const cib = centsFromMoney(getCell(r, "cib-price", "cib_price", "cib_price_cents"));
    const newp = centsFromMoney(getCell(r, "new-price", "new_price", "new_price_cents"));
    const graded = centsFromMoney(getCell(r, "graded-price", "graded_price", "graded_price_cents"));
    const boxOnly = centsFromMoney(getCell(r, "box-only-price", "box_only_price", "box_only_price_cents"));
    const manualOnly = centsFromMoney(getCell(r, "manual-only-price", "manual_only_price", "manual_only_price_cents"));
    const bgs10 = centsFromMoney(getCell(r, "bgs-10-price", "bgs_10_price", "bgs_10_price_cents"));
    const cgc10 = centsFromMoney(getCell(r, "cgc-10-price", "cgc_10_price", "cgc_10_price_cents"));
    const psa10 = centsFromMoney(getCell(r, "psa-10-price", "psa_10_price", "psa_10_price_cents"));

    const rawObj = {};
    headers.forEach((h, j) => {
      rawObj[h] = r[j] ?? "";
    });

    await client.query(upsertSql, [
      game,
      pricecharting_id,
      product_name,
      console_name,
      loose,
      cib,
      newp,
      graded,
      boxOnly,
      manualOnly,
      bgs10,
      cgc10,
      psa10,
      release_date,
      sourceDate,
      rawObj,
    ]);
    inserted++;

    if (inserted % 5000 === 0) {
      console.log(`... processed ${inserted} rows`);
    }
  }

  await client.query(
    `UPDATE public.pricecharting_import_runs
     SET row_count = $2
     WHERE id = $1`,
    [runId, inserted]
  );

  await client.end();
  console.log(`✅ Imported ${inserted} rows into pricecharting_prices_raw (run_id=${runId})`);
})().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
