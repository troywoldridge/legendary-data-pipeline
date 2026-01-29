#!/usr/bin/env node
/* eslint-disable no-console */
/**
 * scripts/pricing/10_rollup_market_values_daily.mjs
 *
 * Roll up market_sales_comps into market_values_daily for CURRENT_DATE.
 * - Uses last 180 days
 * - Median + p25/p75 + last sale + confidence grade
 *
 * Env:
 *   DATABASE_URL (required)
 */

import "dotenv/config";
import pg from "pg";

const { Client } = pg;

const DATABASE_URL = process.env.DATABASE_URL || "";
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL");
  process.exit(1);
}

async function main() {
  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();

  try {
    console.log("=== rollup_market_values_daily: start ===");

    const sql = `
      WITH recent AS (
        SELECT *
        FROM public.market_sales_comps
        WHERE sold_at >= now() - interval '180 days'
      ),
      stats AS (
        SELECT
          card_key,
          grade,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY sold_price_usd) AS median_price,
          percentile_cont(0.25) WITHIN GROUP (ORDER BY sold_price_usd) AS p25,
          percentile_cont(0.75) WITHIN GROUP (ORDER BY sold_price_usd) AS p75,
          COUNT(*)::int AS cnt,
          MAX(sold_at) AS last_sale_at
        FROM recent
        GROUP BY card_key, grade
      )
      INSERT INTO public.market_values_daily (
        as_of_date,
        card_key,
        grade,
        market_value_usd,
        range_low_usd,
        range_high_usd,
        last_sale_usd,
        last_sale_at,
        sales_count_180d,
        confidence
      )
      SELECT
        CURRENT_DATE,
        s.card_key,
        s.grade,
        s.median_price::numeric(12,2),
        s.p25::numeric(12,2),
        s.p75::numeric(12,2),
        (
          SELECT r.sold_price_usd
          FROM recent r
          WHERE r.card_key = s.card_key AND r.grade = s.grade
          ORDER BY r.sold_at DESC
          LIMIT 1
        )::numeric(12,2) AS last_sale_usd,
        s.last_sale_at,
        s.cnt AS sales_count_180d,
        CASE
          WHEN s.cnt >= 10 THEN 'A'
          WHEN s.cnt >= 5 THEN 'B'
          WHEN s.cnt >= 2 THEN 'C'
          ELSE 'D'
        END AS confidence
      FROM stats s
      ON CONFLICT (as_of_date, card_key, grade) DO UPDATE SET
        market_value_usd = EXCLUDED.market_value_usd,
        range_low_usd = EXCLUDED.range_low_usd,
        range_high_usd = EXCLUDED.range_high_usd,
        last_sale_usd = EXCLUDED.last_sale_usd,
        last_sale_at = EXCLUDED.last_sale_at,
        sales_count_180d = EXCLUDED.sales_count_180d,
        confidence = EXCLUDED.confidence
    `;

    const res = await client.query(sql);
    console.log("rows affected:", res.rowCount ?? "(unknown)");
    console.log("=== rollup_market_values_daily: done ===");
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error("Failed:", err?.stack || err?.message || String(err));
  process.exit(1);
});
