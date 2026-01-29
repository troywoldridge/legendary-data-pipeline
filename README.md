# legendary-data-pipeline

A curated, public-safe showcase of the **Legendary Collectibles** data pipeline scripts.

This repo focuses on real-world ETL workflows:
- Importing pricing data (CSV sources)
- Normalizing and rolling up daily market values
- Exporting commerce feeds (Google Merchant)
- Revaluation jobs to keep collection values current

> This is a sanitized showcase repo. Secrets are not stored here. Some private integrations are intentionally omitted.

## Folder layout
- `scripts/pricing/` — pricing import/normalize/rollup pipeline steps
- `scripts/feeds/` — feed export tooling (Google Merchant)
- `scripts/revalue/` — collection revaluation jobs
- `docs/` — pipeline notes and runbooks

## Environment
Copy `.env.example` to `.env` and fill in values.

## Run examples
Pricing pipeline:
- `node scripts/pricing/01_import_pricecharting_csv.js`
- `node scripts/pricing/02_normalize_scryfall_prices.js`
- `node scripts/pricing/03_build_market_price_daily.js`
- `node scripts/pricing/10_rollup_market_values_daily.mjs`

Feed export:
- `node scripts/feeds/exportGoogleMerchantFeed.mjs`

Revalue:
- `node scripts/revalue/revalueCollection.mjs`
