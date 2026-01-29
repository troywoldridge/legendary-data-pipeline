// scripts/revalueCollection.mjs
// Node 20+
// Uses DATABASE_URL from your .env
//
// Run:
//   node scripts/revalueCollection.mjs

import "dotenv/config";
import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
});

async function main() {
  const client = await pool.connect();
  try {
    console.log("=== Collection revalue: start ===");

    // Date from DB (date only)
    const { rows: dateRows } = await client.query(
      "SELECT CURRENT_DATE::date AS d"
    );
    const asOfDate = dateRows[0]?.d;
    console.log("as_of_date:", asOfDate);

    // Load all collection items
    const { rows: items } = await client.query(`
      SELECT
        id,
        user_id,
        game,
        card_id,
        quantity,
        cost_cents
      FROM user_collection_items
      ORDER BY user_id, game, card_id
    `);

    if (!items.length) {
      console.log("No collection items found. Nothing to do.");
      return;
    }
    console.log(`Found ${items.length} collection items`);

    // Load vendor prices into maps
    const [pokemonPrices, ygoPrices, mtgPrices] = await Promise.all([
      loadPokemonPrices(client),
      loadYgoPrices(client),
      loadMtgPrices(client),
    ]);

    // Aggregators per user for daily snapshot
    const perUser = new Map(); // userId -> { totalQty, distinctItems, totalCostCents, totalValueCents }

    // Per-item updates to last_value_cents
    const updates = [];

    for (const item of items) {
      const price = getPriceForItem(item, {
        pokemonPrices,
        ygoPrices,
        mtgPrices,
      });

      const qty = Number(item.quantity || 0);

      const priceCents =
        price != null && Number.isFinite(price) ? Math.round(price * 100) : null;

      // NOTE: this is per-unit last_value_cents (matches your existing update behavior)
      updates.push({
        id: item.id,
        last_value_cents: priceCents,
      });

      const totalValueCents = priceCents != null ? priceCents * qty : null;

      let agg = perUser.get(item.user_id);
      if (!agg) {
        agg = {
          totalQty: 0,
          distinctItems: 0,
          totalCostCents: 0,
          totalValueCents: 0,
        };
        perUser.set(item.user_id, agg);
      }

      agg.totalQty += qty;
      agg.distinctItems += 1;

      if (item.cost_cents != null) {
        agg.totalCostCents += Number(item.cost_cents) * qty;
      }

      if (totalValueCents != null) {
        agg.totalValueCents += totalValueCents;
      }
    }

    await client.query("BEGIN");

    // --- 2a) Update user_collection_items.last_value_cents in bulk ---
    if (updates.length) {
      // We avoid assuming id is uuid; compare via ::text to be safe
      // Also allow last_value_cents to be null (cast to int via ::int when not null)
      const valuesSql = updates
        .map((_u, i) => `($${i * 2 + 1}::text, $${i * 2 + 2}::int)`)
        .join(", ");

      const params = [];
      for (const u of updates) params.push(String(u.id), u.last_value_cents);

      await client.query(
        `
        UPDATE user_collection_items AS u
        SET last_value_cents = v.last_value_cents,
            updated_at = NOW()
        FROM (VALUES ${valuesSql}) AS v(id, last_value_cents)
        WHERE u.id::text = v.id;
        `,
        params
      );

      console.log(`Updated last_value_cents for ${updates.length} items`);
    }

    // --- 2b) Insert daily valuations per user ---
    if (perUser.size) {
      // Clear existing rows for this date (idempotent reruns)
      await client.query(
        `DELETE FROM user_collection_daily_valuations WHERE as_of_date = $1`,
        [asOfDate]
      );

      const dailyValuesSql = [];
      const dailyParams = [];
      let idx = 1;

      for (const [userId, agg] of perUser.entries()) {
        dailyValuesSql.push(
          `($${idx++}::text, $${idx++}::date, $${idx++}::integer, $${idx++}::integer, $${idx++}::bigint, $${idx++}::bigint)`
        );
        dailyParams.push(
          String(userId),
          asOfDate,
          agg.totalQty,
          agg.distinctItems,
          agg.totalCostCents || 0,
          agg.totalValueCents || 0
        );
      }

      await client.query(
        `
        INSERT INTO user_collection_daily_valuations
          (user_id, as_of_date, total_quantity, distinct_items, total_cost_cents, total_value_cents)
        VALUES ${dailyValuesSql.join(", ")}
        `,
        dailyParams
      );

      console.log(`Inserted daily valuations for ${perUser.size} user(s)`);
    }

    await client.query("COMMIT");
    console.log("=== Collection revalue: done ===");
  } catch (err) {
    console.error("Revalue failed:", err);
    try {
      await client.query("ROLLBACK");
    } catch {
      // ignore
    }
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

// ---- Price loaders --------------------------------------------------

async function loadPokemonPrices(client) {
  // Use numeric columns first, then safely parse variant text if it's numeric.
  const { rows } = await client.query(`
    SELECT
      card_id,
      COALESCE(
        market_price,
        mid_price,
        CASE WHEN normal ~ '^[0-9]+(\\.[0-9]+)?$' THEN normal::numeric END,
        CASE WHEN reverse_holofoil ~ '^[0-9]+(\\.[0-9]+)?$' THEN reverse_holofoil::numeric END,
        CASE WHEN holofoil ~ '^[0-9]+(\\.[0-9]+)?$' THEN holofoil::numeric END,
        CASE WHEN first_edition_holofoil ~ '^[0-9]+(\\.[0-9]+)?$' THEN first_edition_holofoil::numeric END,
        CASE WHEN first_edition_normal ~ '^[0-9]+(\\.[0-9]+)?$' THEN first_edition_normal::numeric END
      ) AS tp_price
    FROM tcg_card_prices_tcgplayer
  `);

  const map = new Map();
  for (const r of rows) {
    if (r.card_id && r.tp_price != null) {
      map.set(r.card_id, Number(r.tp_price));
    }
  }

  // Optional eBay fallback if table exists
  try {
    const { rows: ebayRows } = await client.query(`
      SELECT card_id, median
      FROM tcg_card_prices_ebay
      WHERE game = 'pokemon'
    `);

    for (const r of ebayRows) {
      if (r.card_id && r.median != null && !map.has(r.card_id)) {
        map.set(r.card_id, Number(r.median));
      }
    }
  } catch (e) {
    console.warn("Pokemon eBay fallback skipped:", e?.message || e);
  }

  console.log(`Loaded ${map.size} Pokémon price entries`);
  return map;
}

async function loadYgoPrices(client) {
  const { rows } = await client.query(`
    SELECT
      card_id,
      COALESCE(
        NULLIF(TRIM(tcgplayer_price::text), '')::numeric,
        NULLIF(TRIM(cardmarket_price::text), '')::numeric,
        NULLIF(TRIM(amazon_price::text), '')::numeric,
        NULLIF(TRIM(coolstuffinc_price::text), '')::numeric,
        NULLIF(TRIM(ebay_price::text), '')::numeric
      ) AS price
    FROM ygo_card_prices
  `);

  const map = new Map();
  for (const r of rows) {
    if (r.card_id && r.price != null) {
      map.set(r.card_id, Number(r.price));
    }
  }

  console.log(`Loaded ${map.size} Yu-Gi-Oh! price entries`);
  return map;
}

async function loadMtgPrices(client) {
  const { rows } = await client.query(`
    SELECT
      scryfall_id,
      effective_usd AS price
    FROM mtg_prices_effective
  `);

  const map = new Map();
  for (const r of rows) {
    if (r.scryfall_id && r.price != null) {
      map.set(r.scryfall_id, Number(r.price));
    }
  }

  console.log(`Loaded ${map.size} MTG price entries`);
  return map;
}

// ---- Price selection per item ---------------------------------------

function getPriceForItem(item, maps) {
  const game = String(item.game || "").toLowerCase();
  const cardId = item.card_id;

  if (!cardId) return null;

  if (game === "pokemon") return maps.pokemonPrices.get(cardId) ?? null;
  if (game === "ygo" || game === "yugioh") return maps.ygoPrices.get(cardId) ?? null;
  if (game === "mtg" || game === "magic") return maps.mtgPrices.get(cardId) ?? null;

  return null; // other categories not wired yet
}

// ---- Kick off -------------------------------------------------------

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
