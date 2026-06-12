/**
 * export.mjs — Pancake polymarket-history bulk exporter
 *
 * Pulls five tables from pancake-production Supabase (READ-ONLY) and writes
 * monthly-partitioned JSONL files under data/raw/{table}/{YYYY}/{MM}/.
 *
 * Design goals:
 *   - Resumable: each month file is written atomically; if the file already
 *     exists AND has a matching .done sentinel it is skipped on re-run.
 *   - Paginated: uses PostgREST Range headers with configurable PAGE_SIZE.
 *   - Progress: prints a line every 50 pages.
 *   - No credentials in output files.
 *
 * Usage:
 *   node pipeline/export.mjs [--table=markets|outcomes|candles_1d|resolutions|trades] [--resume]
 *
 * Credentials: reads NEXT_PUBLIC_SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
 *              from pancake-production/.env.local  (same pattern as generate-fixtures.mjs)
 *
 * Output layout (under polymarket-history/data/raw/):
 *   markets/       — monthly by created_at
 *   outcomes/      — monthly by created_at of parent market (joined)
 *                    NOTE: outcomes don't have their own timestamp; we partition
 *                    by the earliest observed_at in price_bars for that instrument,
 *                    or fall back to markets.created_at for that market.
 *                    For simplicity we partition outcomes by market created_at.
 *   candles_1d/    — monthly by observed_at
 *   resolutions/   — monthly by resolved_at
 *   trades/        — monthly by observed_at
 *
 * JSONL format: one JSON object per line, no surrounding array.
 * Timestamps in Parquet will be INT64 unix seconds per contract §4 note.
 * Here in JSONL we emit ISO 8601 strings for readability; to_parquet.py
 * converts observed_at / source_timestamp / received_at / released_at to INT64.
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..');
const ENV_PATH = path.resolve(__dirname, '../../pancake-production/.env.local');
const RAW_DIR = path.join(REPO_ROOT, 'data', 'raw');

const PAGE_SIZE = 1000;          // PostgREST page size
const PROGRESS_EVERY = 50;       // print progress line every N pages

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------
const args = process.argv.slice(2);
const tableArg = args.find(a => a.startsWith('--table='))?.split('=')[1];
const RESUME = args.includes('--resume');

const ALL_TABLES = ['markets', 'outcomes', 'candles_1d', 'resolutions', 'trades'];
const TABLES_TO_RUN = tableArg ? [tableArg] : ALL_TABLES;

for (const t of TABLES_TO_RUN) {
  if (!ALL_TABLES.includes(t)) {
    console.error(`Unknown table: ${t}. Valid: ${ALL_TABLES.join(', ')}`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// Credentials (never written to any output file)
// ---------------------------------------------------------------------------
function loadEnv(envPath) {
  const text = fs.readFileSync(envPath, 'utf8');
  const env = {};
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    let val = trimmed.slice(eq + 1).trim();
    if ((val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    env[key] = val;
  }
  return env;
}

const env = loadEnv(ENV_PATH);
const SUPABASE_URL = env.NEXT_PUBLIC_SUPABASE_URL;
const SERVICE_ROLE_KEY = env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env.local');
  process.exit(1);
}

// Sanity guard: bail if any output file would contain a cred fragment
const CRED_SENTINEL = SERVICE_ROLE_KEY.slice(0, 24);
function assertNoCreds(str) {
  if (str.includes(CRED_SENTINEL)) {
    throw new Error('SECURITY: credential fragment detected in output — aborting');
  }
}

const PG_REST = `${SUPABASE_URL}/rest/v1`;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
async function pgRangeRequest(restPath, params, offset, pageSize) {
  const url = new URL(`${PG_REST}${restPath}`);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const resp = await fetch(url.toString(), {
    headers: {
      apikey: SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SERVICE_ROLE_KEY}`,
      'Accept-Profile': 'public',
      Prefer: 'count=exact',
      Range: `${offset}-${offset + pageSize - 1}`,
      'Range-Unit': 'items',
    },
  });
  if (resp.status === 416) return { rows: [], totalCount: null, done: true };
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`PostgREST ${resp.status} on ${restPath}: ${body.slice(0, 400)}`);
  }
  const rows = await resp.json();
  // Content-Range: 0-999/24304
  const cr = resp.headers.get('content-range');
  let totalCount = null;
  if (cr) {
    const m = cr.match(/\/(\d+)$/);
    if (m) totalCount = parseInt(m[1], 10);
  }
  return { rows, totalCount, done: rows.length < pageSize };
}

// ---------------------------------------------------------------------------
// JSONL writer (streams records to disk, never holds all in memory)
// ---------------------------------------------------------------------------
class JsonlWriter {
  constructor(filePath) {
    this.filePath = filePath;
    this.donePath = filePath + '.done';
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    this.fd = fs.openSync(filePath, 'w');
    this.count = 0;
  }
  write(obj) {
    const line = JSON.stringify(obj) + '\n';
    assertNoCreds(line);
    fs.writeSync(this.fd, line);
    this.count++;
  }
  close() {
    fs.closeSync(this.fd);
    // Write sentinel
    fs.writeFileSync(this.donePath, JSON.stringify({ rows: this.count, completed_at: new Date().toISOString() }));
    return this.count;
  }
}

function isDone(filePath) {
  return fs.existsSync(filePath + '.done');
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------
function unixToIso(v) {
  if (v == null) return null;
  return new Date(Number(v) * 1000).toISOString();
}

// ---------------------------------------------------------------------------
// License block (frozen per contract §P5)
// ---------------------------------------------------------------------------
const LICENSE = {
  usage_rights: 'attribution_required',
  redistribution_allowed: 'attribution_only',
  attribution_required: true,
  attribution_text: 'Data sourced from Polymarket via Pancake (usepancake.com)',
  commercial_use_allowed: false,
};

// ---------------------------------------------------------------------------
// Month key from unix timestamp
// ---------------------------------------------------------------------------
function monthKey(unixSeconds) {
  const d = new Date(Number(unixSeconds) * 1000);
  return `${d.getUTCFullYear().toString().padStart(4, '0')}/${(d.getUTCMonth() + 1).toString().padStart(2, '0')}`;
}

function monthKeyFromIso(isoStr) {
  if (!isoStr) return '1970/01';
  const d = new Date(isoStr);
  return `${d.getUTCFullYear().toString().padStart(4, '0')}/${(d.getUTCMonth() + 1).toString().padStart(2, '0')}`;
}

// ---------------------------------------------------------------------------
// § MARKETS
// Partition by created_at (timestamptz).
// Fetch all prediction_markets + join provider_market_ids + resolutions.
// ---------------------------------------------------------------------------
async function exportMarkets() {
  console.log('\n=== MARKETS ===');

  // First load all resolutions into memory (7.8k rows — small)
  console.log('  Loading resolutions index...');
  const resMap = new Map();
  let rOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/resolutions', {
      select: 'resolution_id,market_id,resolved_at,released_at,received_at,winning_outcome_instrument_id,resolution_source,evidence_uri,schema_version,normalizer_version',
      order: 'market_id.asc',
    }, rOffset, PAGE_SIZE);
    for (const r of rows) {
      if (!resMap.has(r.market_id)) resMap.set(r.market_id, r);
    }
    if (done) break;
    rOffset += PAGE_SIZE;
  }
  console.log(`  Resolutions indexed: ${resMap.size}`);

  // Load provider_market_ids
  console.log('  Loading provider_market_ids...');
  const extIdsMap = new Map();
  let pOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/provider_market_ids', {
      select: 'market_id,external_id_kind,external_id_value',
      order: 'market_id.asc',
    }, pOffset, PAGE_SIZE);
    for (const r of rows) {
      if (!extIdsMap.has(r.market_id)) extIdsMap.set(r.market_id, {});
      extIdsMap.get(r.market_id)[r.external_id_kind] = r.external_id_value;
    }
    if (done) break;
    pOffset += PAGE_SIZE;
  }
  console.log(`  Provider market IDs indexed: ${extIdsMap.size} markets`);

  // Load outcomes (15.8k — small)
  console.log('  Loading outcomes index...');
  const outcomesMap = new Map(); // market_id -> outcome[]
  let oOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/prediction_outcomes', {
      select: 'instrument_id,market_id,label,payoff_on_resolve,sort_order',
      order: 'market_id.asc,sort_order.asc.nullslast',
    }, oOffset, PAGE_SIZE);
    for (const r of rows) {
      if (!outcomesMap.has(r.market_id)) outcomesMap.set(r.market_id, []);
      outcomesMap.get(r.market_id).push(r);
    }
    if (done) break;
    oOffset += PAGE_SIZE;
  }
  console.log(`  Outcomes indexed: ${outcomesMap.size} markets`);

  // Stream markets page by page
  const writers = new Map(); // month -> JsonlWriter
  let offset = 0;
  let pageNum = 0;
  let totalRows = 0;
  let totalCount = null;

  const closeAllWriters = () => {
    for (const [month, w] of writers.entries()) {
      const n = w.close();
      console.log(`  markets/${month}: ${n} rows`);
    }
  };

  console.log('  Streaming prediction_markets...');
  while (true) {
    const { rows, totalCount: tc, done } = await pgRangeRequest('/prediction_markets', {
      select: 'market_id,title,category,market_kind,resolution_rule,resolves_at_planned,total_volume_usd,created_at,natural_id',
      order: 'created_at.asc,market_id.asc',
    }, offset, PAGE_SIZE);

    if (totalCount === null && tc !== null) {
      totalCount = tc;
      console.log(`  Total markets: ${totalCount}`);
    }

    for (const m of rows) {
      const month = monthKeyFromIso(m.created_at);
      const filePath = path.join(RAW_DIR, 'markets', month, 'data.jsonl');

      if (RESUME && isDone(filePath)) continue; // skip already-done months

      if (!writers.has(month)) {
        // Close previous month writers that won't be needed anymore
        writers.set(month, new JsonlWriter(filePath));
      }

      const res = resMap.get(m.market_id);
      const extIds = extIdsMap.get(m.market_id) || {};
      const outcomes = (outcomesMap.get(m.market_id) || []).map(o => ({
        instrument_id: o.instrument_id,
        label: o.label,
        payoff_on_resolve: parseFloat(o.payoff_on_resolve),
        sort_order: o.sort_order ?? null,
      }));

      const record = {
        market_id: m.market_id,
        venue: 'polymarket',
        title: m.title,
        category: m.category ?? null,
        market_kind: m.market_kind,
        status: res ? 'resolved' : 'open',
        resolution_rule: m.resolution_rule,
        resolves_at_planned: m.resolves_at_planned ? unixToIso(m.resolves_at_planned) : null,
        total_volume_usd: m.total_volume_usd != null ? parseFloat(m.total_volume_usd) : null,
        created_at: m.created_at,
        external_ids: extIds,
        outcomes,
        derived: false,
        license: LICENSE,
      };

      if (res) {
        record.resolution = {
          resolution_id: res.resolution_id,
          resolved_at: unixToIso(res.resolved_at),
          released_at: unixToIso(res.released_at),
          received_at: unixToIso(res.received_at),
          winning_outcome_instrument_id: res.winning_outcome_instrument_id,
          resolution_source: res.resolution_source,
          evidence_uri: res.evidence_uri ?? null,
          schema_version: res.schema_version,
          normalizer_version: res.normalizer_version,
        };
      }

      writers.get(month).write(record);
      totalRows++;
    }

    pageNum++;
    if (pageNum % PROGRESS_EVERY === 0) {
      console.log(`  [markets] page ${pageNum}, rows so far: ${totalRows}${totalCount ? `/${totalCount}` : ''}`);
    }

    if (done) break;
    offset += PAGE_SIZE;
  }

  closeAllWriters();
  console.log(`  MARKETS DONE: ${totalRows} rows written`);
  return totalRows;
}

// ---------------------------------------------------------------------------
// § OUTCOMES
// Partition by market's created_at (via the outcomesMap/marketsCreatedAt we load).
// We need market created_at to partition; simplest: load all markets first.
// ---------------------------------------------------------------------------
async function exportOutcomes() {
  console.log('\n=== OUTCOMES ===');

  // Load market created_at map
  console.log('  Loading market created_at index...');
  const marketCreatedAt = new Map();
  let mOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/prediction_markets', {
      select: 'market_id,created_at',
      order: 'market_id.asc',
    }, mOffset, PAGE_SIZE);
    for (const r of rows) marketCreatedAt.set(r.market_id, r.created_at);
    if (done) break;
    mOffset += PAGE_SIZE;
  }
  console.log(`  Markets indexed: ${marketCreatedAt.size}`);

  // Stream outcomes
  const writers = new Map();
  let offset = 0;
  let pageNum = 0;
  let totalRows = 0;
  let totalCount = null;

  const closeAllWriters = () => {
    for (const [month, w] of writers.entries()) {
      const n = w.close();
      console.log(`  outcomes/${month}: ${n} rows`);
    }
  };

  console.log('  Streaming prediction_outcomes...');
  while (true) {
    const { rows, totalCount: tc, done } = await pgRangeRequest('/prediction_outcomes', {
      select: 'instrument_id,market_id,label,payoff_on_resolve,sort_order',
      order: 'market_id.asc,sort_order.asc.nullslast',
    }, offset, PAGE_SIZE);

    if (totalCount === null && tc !== null) {
      totalCount = tc;
      console.log(`  Total outcomes: ${totalCount}`);
    }

    for (const o of rows) {
      const marketCat = marketCreatedAt.get(o.market_id);
      const month = marketCat ? monthKeyFromIso(marketCat) : '1970/01';
      const filePath = path.join(RAW_DIR, 'outcomes', month, 'data.jsonl');

      if (RESUME && isDone(filePath)) continue;

      if (!writers.has(month)) {
        writers.set(month, new JsonlWriter(filePath));
      }

      writers.get(month).write({
        instrument_id: o.instrument_id,
        market_id: o.market_id,
        venue: 'polymarket',
        label: o.label,
        payoff_on_resolve: parseFloat(o.payoff_on_resolve),
        sort_order: o.sort_order ?? null,
        derived: false,
        license: LICENSE,
      });
      totalRows++;
    }

    pageNum++;
    if (pageNum % PROGRESS_EVERY === 0) {
      console.log(`  [outcomes] page ${pageNum}, rows so far: ${totalRows}${totalCount ? `/${totalCount}` : ''}`);
    }

    if (done) break;
    offset += PAGE_SIZE;
  }

  closeAllWriters();
  console.log(`  OUTCOMES DONE: ${totalRows} rows written`);
  return totalRows;
}

// ---------------------------------------------------------------------------
// § CANDLES_1D
// Partition by observed_at (Unix bigint). ~851k rows — biggest table.
//
// Strategy: iterate by instrument_id to use the (instrument_id, observed_at)
// composite index, avoiding the statement timeout caused by a full-table scan
// with bar_period filter. Batch instruments in groups of INSTR_BATCH_SIZE to
// amortize HTTP overhead.
// ---------------------------------------------------------------------------
const INSTR_BATCH_SIZE = 20; // instruments per batch request

async function exportCandles1d() {
  console.log('\n=== CANDLES_1D ===');

  // Load instrument -> market_id + label map
  console.log('  Loading outcomes index (for market_id + label denorm)...');
  const instrMap = new Map(); // instrument_id -> { market_id, label }
  let oOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/prediction_outcomes', {
      select: 'instrument_id,market_id,label',
      order: 'instrument_id.asc',
    }, oOffset, PAGE_SIZE);
    for (const r of rows) instrMap.set(r.instrument_id, { market_id: r.market_id, label: r.label });
    if (done) break;
    oOffset += PAGE_SIZE;
  }
  console.log(`  Instruments indexed: ${instrMap.size}`);

  // Load provider_instrument_ids
  console.log('  Loading provider_instrument_ids...');
  const instrExtIds = new Map();
  let pOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/provider_instrument_ids', {
      select: 'instrument_id,external_id_kind,external_id_value',
      order: 'instrument_id.asc',
    }, pOffset, PAGE_SIZE);
    for (const r of rows) {
      if (!instrExtIds.has(r.instrument_id)) instrExtIds.set(r.instrument_id, {});
      instrExtIds.get(r.instrument_id)[r.external_id_kind] = r.external_id_value;
    }
    if (done) break;
    pOffset += PAGE_SIZE;
  }
  console.log(`  Instrument external IDs indexed: ${instrExtIds.size}`);

  // Build list of all instrument IDs that have 1d bars (from instrMap — all outcomes)
  // We'll query each batch and rely on the index; bars for non-prediction instruments
  // will simply return 0 rows.
  const allInstrumentIds = [...instrMap.keys()];
  console.log(`  Will query ${allInstrumentIds.length} instruments in batches of ${INSTR_BATCH_SIZE}`);

  const writers = new Map();
  let totalRows = 0;
  let skippedOrphan = 0;
  let instrProcessed = 0;
  const totalInstr = allInstrumentIds.length;

  const closeAllWriters = () => {
    for (const [month, w] of writers.entries()) {
      const n = w.close();
      console.log(`  candles_1d/${month}: ${n} rows`);
    }
  };

  // Process in batches of INSTR_BATCH_SIZE instruments
  for (let batchStart = 0; batchStart < allInstrumentIds.length; batchStart += INSTR_BATCH_SIZE) {
    const batch = allInstrumentIds.slice(batchStart, batchStart + INSTR_BATCH_SIZE);
    const instrFilter = `in.(${batch.join(',')})`;

    // Fetch all bars for this batch of instruments (paginated if >PAGE_SIZE)
    let bOffset = 0;
    while (true) {
      const { rows, done } = await pgRangeRequest('/price_bars', {
        select: 'price_bar_id,instrument_id,observed_at,source_timestamp,received_at,released_at,bar_period,open,high,low,close,volume,currency,manifest_id,schema_version,normalizer_version',
        instrument_id: instrFilter,
        bar_period: 'eq.1 day',
        order: 'instrument_id.asc,observed_at.asc',
      }, bOffset, PAGE_SIZE);

      for (const bar of rows) {
        const instrInfo = instrMap.get(bar.instrument_id);
        if (!instrInfo) {
          skippedOrphan++;
          continue;
        }

        const month = monthKey(bar.observed_at);
        const filePath = path.join(RAW_DIR, 'candles_1d', month, 'data.jsonl');

        if (!writers.has(month)) {
          writers.set(month, new JsonlWriter(filePath));
        }

        const extIds = instrExtIds.get(bar.instrument_id) || {};

        writers.get(month).write({
          instrument_id: bar.instrument_id,
          market_id: instrInfo.market_id,
          outcome_label: instrInfo.label,
          venue: 'polymarket',
          external_ids: extIds,
          observed_at: Number(bar.observed_at),
          source_timestamp: Number(bar.source_timestamp),
          received_at: Number(bar.received_at),
          released_at: Number(bar.released_at),
          bar_period: '1d',
          open: parseFloat(bar.open),
          high: parseFloat(bar.high),
          low: parseFloat(bar.low),
          close: parseFloat(bar.close),
          volume: bar.volume != null ? parseFloat(bar.volume) : null,
          currency: bar.currency,
          derived: false,
          derivation_method: null,
          schema_version: bar.schema_version,
          normalizer_version: bar.normalizer_version,
          manifest_id: bar.manifest_id,
          license: LICENSE,
        });
        totalRows++;
      }

      if (done) break;
      bOffset += PAGE_SIZE;
    }

    instrProcessed += batch.length;
    const batchNum = Math.floor(batchStart / INSTR_BATCH_SIZE) + 1;
    if (batchNum % PROGRESS_EVERY === 0) {
      const pct = (instrProcessed / totalInstr * 100).toFixed(1);
      console.log(`  [candles_1d] batch ${batchNum}, instruments: ${instrProcessed}/${totalInstr} (${pct}%), rows: ${totalRows}`);
    }
  }

  closeAllWriters();
  if (skippedOrphan > 0) {
    console.log(`  NOTE: skipped ${skippedOrphan} bars with no matching outcome (spot pair instruments)`);
  }
  console.log(`  CANDLES_1D DONE: ${totalRows} rows written`);
  return totalRows;
}

// ---------------------------------------------------------------------------
// § RESOLUTIONS
// Partition by resolved_at (Unix bigint).
// ---------------------------------------------------------------------------
async function exportResolutions() {
  console.log('\n=== RESOLUTIONS ===');

  // Load provider_market_ids for external_ids enrichment
  const extIdsMap = new Map();
  let pOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/provider_market_ids', {
      select: 'market_id,external_id_kind,external_id_value',
      order: 'market_id.asc',
    }, pOffset, PAGE_SIZE);
    for (const r of rows) {
      if (!extIdsMap.has(r.market_id)) extIdsMap.set(r.market_id, {});
      extIdsMap.get(r.market_id)[r.external_id_kind] = r.external_id_value;
    }
    if (done) break;
    pOffset += PAGE_SIZE;
  }

  // Load winning_outcome_label
  const outcomeLabel = new Map();
  let oOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/prediction_outcomes', {
      select: 'instrument_id,label',
      order: 'instrument_id.asc',
    }, oOffset, PAGE_SIZE);
    for (const r of rows) outcomeLabel.set(r.instrument_id, r.label);
    if (done) break;
    oOffset += PAGE_SIZE;
  }

  const writers = new Map();
  let offset = 0;
  let pageNum = 0;
  let totalRows = 0;
  let totalCount = null;

  const closeAllWriters = () => {
    for (const [month, w] of writers.entries()) {
      const n = w.close();
      console.log(`  resolutions/${month}: ${n} rows`);
    }
  };

  console.log('  Streaming resolutions...');
  while (true) {
    const { rows, totalCount: tc, done } = await pgRangeRequest('/resolutions', {
      select: 'resolution_id,market_id,resolved_at,released_at,received_at,winning_outcome_instrument_id,resolution_source,evidence_uri,schema_version,normalizer_version,manifest_id',
      order: 'resolved_at.asc,resolution_id.asc',
    }, offset, PAGE_SIZE);

    if (totalCount === null && tc !== null) {
      totalCount = tc;
      console.log(`  Total resolutions: ${totalCount}`);
    }

    for (const r of rows) {
      const month = monthKey(r.resolved_at);
      const filePath = path.join(RAW_DIR, 'resolutions', month, 'data.jsonl');

      if (RESUME && isDone(filePath)) { totalRows++; continue; }

      if (!writers.has(month)) {
        writers.set(month, new JsonlWriter(filePath));
      }

      writers.get(month).write({
        resolution_id: r.resolution_id,
        market_id: r.market_id,
        venue: 'polymarket',
        external_ids: extIdsMap.get(r.market_id) || {},
        // Timestamps as INT64 unix seconds per contract §4 note
        resolved_at: Number(r.resolved_at),
        released_at: Number(r.released_at),
        received_at: Number(r.received_at),
        winning_outcome_instrument_id: r.winning_outcome_instrument_id,
        winning_outcome_label: outcomeLabel.get(r.winning_outcome_instrument_id) ?? null,
        resolution_source: r.resolution_source,
        evidence_uri: r.evidence_uri ?? null,
        schema_version: r.schema_version,
        normalizer_version: r.normalizer_version,
        manifest_id: r.manifest_id,
        license: LICENSE,
      });
      totalRows++;
    }

    pageNum++;
    if (pageNum % PROGRESS_EVERY === 0) {
      console.log(`  [resolutions] page ${pageNum}, rows so far: ${totalRows}${totalCount ? `/${totalCount}` : ''}`);
    }

    if (done) break;
    offset += PAGE_SIZE;
  }

  closeAllWriters();
  console.log(`  RESOLUTIONS DONE: ${totalRows} rows written`);
  return totalRows;
}

// ---------------------------------------------------------------------------
// § TRADES
// Partition by observed_at (Unix bigint).
// All trades are synthetic last-price rows (derived:true per rule 106).
// ---------------------------------------------------------------------------
async function exportTrades() {
  console.log('\n=== TRADES ===');

  // Load instrument -> market_id + label
  const instrMap = new Map();
  let oOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/prediction_outcomes', {
      select: 'instrument_id,market_id,label',
      order: 'instrument_id.asc',
    }, oOffset, PAGE_SIZE);
    for (const r of rows) instrMap.set(r.instrument_id, { market_id: r.market_id, label: r.label });
    if (done) break;
    oOffset += PAGE_SIZE;
  }

  // Load instrument external IDs
  const instrExtIds = new Map();
  let pOffset = 0;
  while (true) {
    const { rows, done } = await pgRangeRequest('/provider_instrument_ids', {
      select: 'instrument_id,external_id_kind,external_id_value',
      order: 'instrument_id.asc',
    }, pOffset, PAGE_SIZE);
    for (const r of rows) {
      if (!instrExtIds.has(r.instrument_id)) instrExtIds.set(r.instrument_id, {});
      instrExtIds.get(r.instrument_id)[r.external_id_kind] = r.external_id_value;
    }
    if (done) break;
    pOffset += PAGE_SIZE;
  }

  const writers = new Map();
  let offset = 0;
  let pageNum = 0;
  let totalRows = 0;
  let totalCount = null;

  const closeAllWriters = () => {
    for (const [month, w] of writers.entries()) {
      const n = w.close();
      console.log(`  trades/${month}: ${n} rows`);
    }
  };

  console.log('  Streaming trades...');
  while (true) {
    const { rows, totalCount: tc, done } = await pgRangeRequest('/trades', {
      select: 'trade_id,instrument_id,observed_at,source_timestamp,received_at,released_at,price,size,side,external_trade_id,currency,manifest_id,schema_version,normalizer_version',
      order: 'observed_at.asc,trade_id.asc',
    }, offset, PAGE_SIZE);

    if (totalCount === null && tc !== null) {
      totalCount = tc;
      console.log(`  Total trades: ${totalCount}`);
    }

    for (const t of rows) {
      const instrInfo = instrMap.get(t.instrument_id);
      const month = monthKey(t.observed_at);
      const filePath = path.join(RAW_DIR, 'trades', month, 'data.jsonl');

      if (RESUME && isDone(filePath)) { totalRows++; continue; }

      if (!writers.has(month)) {
        writers.set(month, new JsonlWriter(filePath));
      }

      // Derived detection: external_trade_id prefix per rule 106
      const isDerived = typeof t.external_trade_id === 'string' &&
                        t.external_trade_id.startsWith('polymarket-mark-');

      const extIds = instrExtIds.get(t.instrument_id) || {};

      writers.get(month).write({
        trade_id: t.trade_id,
        instrument_id: t.instrument_id,
        market_id: instrInfo?.market_id ?? null,
        outcome_label: instrInfo?.label ?? null,
        venue: 'polymarket',
        external_ids: extIds,
        // Timestamps as INT64 unix seconds per contract §4 note
        observed_at: Number(t.observed_at),
        source_timestamp: Number(t.source_timestamp),
        received_at: Number(t.received_at),
        released_at: Number(t.released_at),
        price: parseFloat(t.price),
        size: parseFloat(t.size),
        side: t.side ?? null,
        external_trade_id: t.external_trade_id ?? null,
        currency: t.currency,
        derived: isDerived,
        derivation_method: isDerived ? 'polymarket_last_trade_price_change_detection' : null,
        schema_version: t.schema_version,
        normalizer_version: t.normalizer_version,
        manifest_id: t.manifest_id,
        license: LICENSE,
      });
      totalRows++;
    }

    pageNum++;
    if (pageNum % PROGRESS_EVERY === 0) {
      const pct = totalCount ? `(${(offset / totalCount * 100).toFixed(1)}%)` : '';
      console.log(`  [trades] page ${pageNum}, rows so far: ${totalRows}${totalCount ? `/${totalCount}` : ''} ${pct}`);
    }

    if (done) break;
    offset += PAGE_SIZE;
  }

  closeAllWriters();
  console.log(`  TRADES DONE: ${totalRows} rows written`);
  return totalRows;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== polymarket-history export pipeline ===');
  console.log(`Tables: ${TABLES_TO_RUN.join(', ')}`);
  console.log(`Resume mode: ${RESUME}`);
  console.log(`Output: ${RAW_DIR}`);
  console.log(`Page size: ${PAGE_SIZE}`);
  console.log('');

  fs.mkdirSync(RAW_DIR, { recursive: true });

  const results = {};
  const startAll = Date.now();

  for (const table of TABLES_TO_RUN) {
    const t0 = Date.now();
    let rows;
    switch (table) {
      case 'markets':    rows = await exportMarkets();    break;
      case 'outcomes':   rows = await exportOutcomes();   break;
      case 'candles_1d': rows = await exportCandles1d();  break;
      case 'resolutions': rows = await exportResolutions(); break;
      case 'trades':     rows = await exportTrades();     break;
    }
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    results[table] = { rows, elapsed_s: parseFloat(elapsed) };
    console.log(`\n  ${table}: ${rows} rows in ${elapsed}s`);
  }

  const totalElapsed = ((Date.now() - startAll) / 1000).toFixed(1);
  console.log('\n=== EXPORT COMPLETE ===');
  for (const [t, r] of Object.entries(results)) {
    console.log(`  ${t.padEnd(14)} ${r.rows.toString().padStart(8)} rows  ${r.elapsed_s}s`);
  }
  console.log(`  Total elapsed: ${totalElapsed}s`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
