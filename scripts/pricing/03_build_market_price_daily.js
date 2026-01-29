#!/usr/bin/env node
/* eslint-disable no-console */
/**
 * scripts/pricing/03_build_market_price_daily.js
 *
 * Build market_price_daily from market_price_snapshots.
 *
 * Rules:
 * - For each (market_item_id, as_of_date, currency) pick the "best" snapshot ON THAT SAME DATE.
 * - UPSERT into market_price_daily.
 *
 * Usage:
 *   node scripts/pricing/03_build_market_price_daily.js
 *   node scripts/pricing/03_build_market_price_daily.js --date 2025-12-19
 *   node scripts/pricing/03_build_market_price_daily.js --currency USD
 *   node scripts/pricing/03_build_market_price_daily.js --all-dates
 *   node scripts/pricing/03_build_market_price_daily.js --since 2025-12-01
 *   node scripts/pricing/03_build_market_price_daily.js --since 2025-12-01 --until 2025-12-31
 *
 * Env:
 *   DATABASE_URL=postgres://...
 */

const pg = require("pg");
const { Pool } = pg;

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

function ymdUtcToday() {
  return new Date().toISOString().slice(0, 10);
}

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL not set");
  process.exit(1);
}

(async function main() {
  const args = parseArgs(process.argv);

  const currency = String(args.currency || "USD").toUpperCase();
  const today = ymdUtcToday();

  const allDates = !!args["all-dates"];
  const asOfDate = String(args.date || today);
  const since = args.since ? String(args.since) : null;
  const until = args.until ? String(args.until) : null;

  const pool = new Pool({
    connectionString: DATABASE_URL,
    max: 5,
  });

  const rangeDesc = allDates
    ? since && until
      ? `for ALL dates ${since} → ${until}`
      : since
        ? `for ALL dates since ${since}`
        : until
          ? `for ALL dates up to ${until}`
          : "for ALL dates in snapshots"
    : `for ${asOfDate}`;

  console.log(`📊 Building market_price_daily (${currency}) ${rangeDesc}`);

  // Build WHERE clause in a way that keeps indexes useful.
  // (Avoids "($bool OR ...)" patterns that force less optimal plans.)
  const whereParts = [`s.currency = $1`];
  const params = [currency];
  let p = 2;

  if (!allDates) {
    whereParts.push(`s.as_of_date = $${p++}::date`);
    params.push(asOfDate);
  } else {
    if (since) {
      whereParts.push(`s.as_of_date >= $${p++}::date`);
      params.push(since);
    }
    if (until) {
      whereParts.push(`s.as_of_date <= $${p++}::date`);
      params.push(until);
    }
  }

  const whereSql = whereParts.join("\n    AND ");

  // NOTE: ranking is PER DAY (partition includes as_of_date).
  // We keep your priority ordering and tie-break on higher value.
  const sql = `
WITH candidates AS (
  SELECT
    s.market_item_id,
    s.currency,
    s.as_of_date,
    s.value_cents,
    s.source,
    s.price_type,
    s.condition,
    s.raw,
    ROW_NUMBER() OVER (
      PARTITION BY s.market_item_id, s.currency, s.as_of_date
      ORDER BY
        CASE s.source
          WHEN 'tcgplayer' THEN 10
          WHEN 'scryfall' THEN 20
          WHEN 'cardmarket' THEN 30
          WHEN 'pricecharting' THEN 40
          WHEN 'ebay' THEN 50
          WHEN 'amazon' THEN 60
          ELSE 99
        END ASC,
        CASE s.price_type
          WHEN 'market' THEN 10
          WHEN 'trend' THEN 12
          WHEN 'mid' THEN 14
          WHEN 'avg_7d' THEN 16
          WHEN 'avg_30d' THEN 18
          WHEN 'low' THEN 22
          WHEN 'high' THEN 24
          WHEN 'loose' THEN 30
          WHEN 'cib' THEN 32
          WHEN 'new' THEN 34
          WHEN 'graded' THEN 36
          WHEN 'foil' THEN 60
          WHEN 'etched' THEN 62
          WHEN 'tix' THEN 80
          ELSE 90
        END ASC,
        s.value_cents DESC
    ) AS rn
  FROM public.market_price_snapshots s
  WHERE
    ${whereSql}
),
best AS (
  SELECT
    market_item_id,
    as_of_date,
    currency,
    value_cents,
    70::int AS confidence,
    jsonb_build_array(
      jsonb_build_object(
        'source', source,
        'price_type', price_type,
        'condition', condition,
        'value_cents', value_cents
      )
    ) AS sources_used,
    'priority_best_of_day'::text AS method
  FROM candidates
  WHERE rn = 1
)
INSERT INTO public.market_price_daily (
  market_item_id,
  as_of_date,
  currency,
  value_cents,
  confidence,
  sources_used,
  method,
  updated_at
)
SELECT
  market_item_id,
  as_of_date,
  currency,
  value_cents,
  confidence,
  sources_used,
  method,
  now()
FROM best
ON CONFLICT (market_item_id, as_of_date, currency)
DO UPDATE SET
  value_cents = EXCLUDED.value_cents,
  confidence = EXCLUDED.confidence,
  sources_used = EXCLUDED.sources_used,
  method = EXCLUDED.method,
  updated_at = now()
`;

  const client = await pool.connect();
  try {
    const res = await client.query(sql, params);
    console.log(`✅ Upserted ${res.rowCount} daily rows into market_price_daily`);
  } finally {
    client.release();
    await pool.end();
  }
})().catch((err) => {
  console.error("❌ Error:", err?.stack || err?.message || err);
  process.exit(1);
});
