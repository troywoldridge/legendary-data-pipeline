#!/usr/bin/env node
/* eslint-disable no-console */
/**
 * scripts/pricing/02_normalize_scryfall_prices.js
 *
 * Normalize Scryfall MTG prices into market_price_snapshots.
 *
 * Source:
 *   public.scryfall_cards_raw.payload->'prices'
 *
 * Join:
 *   market_items(game='mtg', canonical_source='scryfall', canonical_id = scryfall_cards_raw.id::text)
 *
 * Behavior:
 *   - Writes snapshots for as_of_date (default: today UTC) for:
 *       usd -> (USD, market)
 *       usd_foil -> (USD, foil)
 *       usd_etched -> (USD, etched)
 *       eur -> (EUR, market)
 *       tix -> (USD, tix)  ✅ treat tix as a USD-like snapshot but tagged by price_type
 *   - Idempotent even WITHOUT a unique index:
 *       1) UPDATE existing rows for the day/source/key
 *       2) INSERT missing rows
 *
 * Usage:
 *   node scripts/pricing/02_normalize_scryfall_prices.js
 *   node scripts/pricing/02_normalize_scryfall_prices.js --date 2025-12-19
 *
 * Env:
 *   DATABASE_URL=postgres://...
 */

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

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL not set");
  process.exit(1);
}

function todayUtcYmd() {
  return new Date().toISOString().slice(0, 10);
}

(async function main() {
  const args = parseArgs(process.argv);
  const asOfDate = String(args.date || "").trim() || todayUtcYmd();

  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();

  console.log(`📥 Normalizing Scryfall MTG prices into market_price_snapshots for ${asOfDate}`);

  // Build a set of normalized rows via SQL (fast), then:
  // 1) UPDATE existing matching keys
  // 2) INSERT missing keys
  //
  // Key = (market_item_id, source, as_of_date, currency, price_type, condition)
  // condition is NULL here.

  const buildRowsCte = `
    WITH src AS (
      SELECT
        mi.id AS market_item_id,
        scr.payload->'prices' AS prices
      FROM public.market_items mi
      JOIN public.scryfall_cards_raw scr
        ON scr.id::text = mi.canonical_id
      WHERE mi.game = 'mtg'
        AND mi.canonical_source = 'scryfall'
        AND scr.payload ? 'prices'
    ),
    rows AS (
      SELECT
        market_item_id,
        'scryfall'::text AS source,
        $1::date AS as_of_date,
        v.currency,
        v.price_type,
        NULL::text AS condition,
        v.value_cents,
        v.raw
      FROM src
      CROSS JOIN LATERAL (
        VALUES
          -- usd
          (
            'USD'::text,
            'market'::text,
            CASE
              WHEN NULLIF(regexp_replace((prices->>'usd')::text, '[^0-9.\\-]', '', 'g'), '') IS NULL THEN NULL
              ELSE (ROUND((NULLIF(regexp_replace((prices->>'usd')::text, '[^0-9.\\-]', '', 'g'), '')::numeric) * 100))::int
            END,
            jsonb_build_object('prices', prices, 'key', 'usd')
          ),
          -- usd_foil
          (
            'USD'::text,
            'foil'::text,
            CASE
              WHEN NULLIF(regexp_replace((prices->>'usd_foil')::text, '[^0-9.\\-]', '', 'g'), '') IS NULL THEN NULL
              ELSE (ROUND((NULLIF(regexp_replace((prices->>'usd_foil')::text, '[^0-9.\\-]', '', 'g'), '')::numeric) * 100))::int
            END,
            jsonb_build_object('prices', prices, 'key', 'usd_foil')
          ),
          -- usd_etched
          (
            'USD'::text,
            'etched'::text,
            CASE
              WHEN NULLIF(regexp_replace((prices->>'usd_etched')::text, '[^0-9.\\-]', '', 'g'), '') IS NULL THEN NULL
              ELSE (ROUND((NULLIF(regexp_replace((prices->>'usd_etched')::text, '[^0-9.\\-]', '', 'g'), '')::numeric) * 100))::int
            END,
            jsonb_build_object('prices', prices, 'key', 'usd_etched')
          ),
          -- eur
          (
            'EUR'::text,
            'market'::text,
            CASE
              WHEN NULLIF(regexp_replace((prices->>'eur')::text, '[^0-9.\\-]', '', 'g'), '') IS NULL THEN NULL
              ELSE (ROUND((NULLIF(regexp_replace((prices->>'eur')::text, '[^0-9.\\-]', '', 'g'), '')::numeric) * 100))::int
            END,
            jsonb_build_object('prices', prices, 'key', 'eur')
          ),
          -- tix (store as USD currency but tagged price_type='tix')
          (
            'USD'::text,
            'tix'::text,
            CASE
              WHEN NULLIF(regexp_replace((prices->>'tix')::text, '[^0-9.\\-]', '', 'g'), '') IS NULL THEN NULL
              ELSE (ROUND((NULLIF(regexp_replace((prices->>'tix')::text, '[^0-9.\\-]', '', 'g'), '')::numeric) * 100))::int
            END,
            jsonb_build_object('prices', prices, 'key', 'tix')
          )
      ) AS v(currency, price_type, value_cents, raw)
      WHERE v.value_cents IS NOT NULL
        AND v.value_cents > 0
    )
  `;

  // 1) UPDATE existing rows for that key/day
  const updateSql = `
    ${buildRowsCte}
    UPDATE public.market_price_snapshots t
    SET
      value_cents = r.value_cents,
      raw = r.raw
    FROM rows r
    WHERE t.market_item_id = r.market_item_id
      AND t.source = r.source
      AND t.as_of_date = r.as_of_date
      AND t.currency = r.currency
      AND t.price_type = r.price_type
      AND t.condition IS NOT DISTINCT FROM r.condition
  `;

  // 2) INSERT missing rows
  const insertSql = `
    ${buildRowsCte}
    INSERT INTO public.market_price_snapshots
      (market_item_id, source, as_of_date, currency, price_type, condition, value_cents, raw)
    SELECT
      r.market_item_id, r.source, r.as_of_date, r.currency, r.price_type, r.condition, r.value_cents, r.raw
    FROM rows r
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.market_price_snapshots t
      WHERE t.market_item_id = r.market_item_id
        AND t.source = r.source
        AND t.as_of_date = r.as_of_date
        AND t.currency = r.currency
        AND t.price_type = r.price_type
        AND t.condition IS NOT DISTINCT FROM r.condition
    )
  `;

  await client.query("BEGIN");
  try {
    const upd = await client.query(updateSql, [asOfDate]);
    const ins = await client.query(insertSql, [asOfDate]);
    await client.query("COMMIT");

    console.log(`✅ Updated ${upd.rowCount} existing snapshot rows`);
    console.log(`✅ Inserted ${ins.rowCount} new snapshot rows`);
    console.log(`✅ Total affected ${upd.rowCount + ins.rowCount}`);
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    await client.end();
  }
})().catch((err) => {
  console.error("❌ Error normalizing Scryfall prices:", err?.stack || err?.message || err);
  process.exit(1);
});
