#!/usr/bin/env node
/**
 * etl_pipeline.js — Production-style End-to-End ETL
 *
 * Stages:
 *   1. Read CSV from disk (or generate synthetic data)
 *   2. Upload raw CSV  → MinIO  warehouse-raw  (Hive partitioned)
 *   3. Transform       → Silver layer (typing, DQ, feature engineering)
 *   4. Write Parquet   → /tmp/  (parquetjs-lite) OR JSONL fallback
 *   5. Upload Parquet  → MinIO  warehouse-silver  (partitioned)
 *   6. Aggregate       → Gold   (hourly KPIs per date/zone)
 *   7. Upload Parquet  → MinIO  warehouse-gold
 *   8. Write Iceberg metadata manifest  → warehouse-silver/_iceberg/
 *
 * Usage:
 *   node scripts/etl_pipeline.js [options]
 *
 * Options:
 *   --csv <path>       Source CSV file  (default: /tmp/etl_test_data.csv)
 *   --generate         Generate 2000-row synthetic NYC taxi CSV
 *   --minio <url>      MinIO endpoint   (default: http://localhost:9000)
 *   --wait             Wait for MinIO to become healthy (up to 60s)
 *   --dry-run          Skip all MinIO uploads (local only)
 *
 * Environment:
 *   MINIO_ENDPOINT     override endpoint
 *   MINIO_ACCESS_KEY   (default: minioadmin)
 *   MINIO_SECRET_KEY   (default: minioadmin123)
 *   AWS_REGION         (default: us-east-1)
 */

'use strict';

const fs     = require('fs');
const path   = require('path');
const http   = require('http');
const https  = require('https');
const crypto = require('crypto');

// ── CLI / Config ──────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const flag  = (f)    => argv.includes(f);
const opt   = (f, d) => { const i = argv.indexOf(f); return i >= 0 ? argv[i + 1] : d; };

const MINIO_ENDPOINT = opt('--minio', process.env.MINIO_ENDPOINT || 'http://localhost:9000');
const ACCESS_KEY     = process.env.MINIO_ACCESS_KEY || 'minioadmin';
const SECRET_KEY     = process.env.MINIO_SECRET_KEY || 'minioadmin123';
const REGION         = process.env.AWS_REGION       || 'us-east-1';
const DRY_RUN        = flag('--dry-run');
const WAIT_MINIO     = flag('--wait');
const GENERATE       = flag('--generate');
const CSV_PATH       = opt('--csv', '/tmp/etl_test_data.csv');

const BUCKETS = {
  raw:    'warehouse-raw',
  silver: 'warehouse-silver',
  gold:   'warehouse-gold',
};

// ── Terminal colours ──────────────────────────────────────────────────────────
const C = {
  reset:  '\x1b[0m',  bold:  '\x1b[1m',
  green:  '\x1b[32m', cyan:  '\x1b[36m',
  yellow: '\x1b[33m', red:   '\x1b[31m',
  blue:   '\x1b[34m', grey:  '\x1b[90m',
  white:  '\x1b[97m',
};

const log  = (m)        => console.log(`${C.grey}[${ts()}]${C.reset} ${C.blue}│${C.reset} ${m}`);
const ok   = (m)        => console.log(`${C.grey}[${ts()}]${C.reset} ${C.green}✓${C.reset} ${m}`);
const warn = (m)        => console.log(`${C.grey}[${ts()}]${C.reset} ${C.yellow}⚠${C.reset} ${m}`);
const fail = (m)        => console.log(`${C.grey}[${ts()}]${C.reset} ${C.red}✗${C.reset} ${m}`);
const head = (n, total) => {
  const bar   = '─'.repeat(55);
  const inner = `  Stage ${n}/7  ${total}`;
  const pad   = Math.max(0, 55 - inner.length);
  console.log(`\n${C.bold}${C.cyan}┌${bar}┐${C.reset}`);
  console.log(`${C.bold}${C.cyan}│${C.reset}${C.white}${C.bold}${inner}${C.reset}${' '.repeat(pad)}${C.bold}${C.cyan}│${C.reset}`);
  console.log(`${C.bold}${C.cyan}└${bar}┘${C.reset}`);
};

function ts() {
  return new Date().toISOString().slice(11, 23);
}

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

// ── CSV Parser ────────────────────────────────────────────────────────────────
function parseCSV(text) {
  const lines   = text.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
  const rows    = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const vals = line.split(',');
    const row  = {};
    headers.forEach((h, j) => { row[h] = (vals[j] || '').trim().replace(/"/g, ''); });
    rows.push(row);
  }
  return rows;
}

// ── Synthetic data generator ──────────────────────────────────────────────────
function generateCSV(n = 2000) {
  const hdr = [
    'VendorID','tpep_pickup_datetime','tpep_dropoff_datetime',
    'passenger_count','trip_distance','RatecodeID','store_and_fwd_flag',
    'PULocationID','DOLocationID','payment_type','fare_amount','extra',
    'mta_tax','tip_amount','tolls_amount','improvement_surcharge',
    'total_amount','congestion_surcharge',
  ];

  const rand    = (lo, hi) => Math.random() * (hi - lo) + lo;
  const randInt = (lo, hi) => Math.floor(rand(lo, hi + 1));
  const pad     = v        => String(v).padStart(2, '0');
  const fmt     = d        => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;

  const base = new Date('2024-01-01T00:00:00Z');
  const rows = [hdr.join(',')];

  for (let i = 0; i < n; i++) {
    const pickup   = new Date(base.getTime() + randInt(0, 27) * 86400000 + randInt(0, 86399) * 1000);
    const dur      = randInt(3, 90);
    const dropoff  = new Date(pickup.getTime() + dur * 60000);
    const dist     = +rand(0.5, 25).toFixed(2);
    const fare     = +(2.5 + dist * 2.5 + rand(0, 5)).toFixed(2);
    const tip      = +(fare * rand(0, 0.3)).toFixed(2);
    const total    = +(fare + tip + 1.3).toFixed(2);

    rows.push([
      randInt(1, 2), fmt(pickup), fmt(dropoff), randInt(1, 6),
      dist, 1, 'N', randInt(1, 265), randInt(1, 265),
      randInt(1, 4), fare, 0.5, 0.5, tip, 0, 0.3, total, 2.5,
    ].join(','));
  }
  return rows.join('\n');
}

// ── Silver transform ──────────────────────────────────────────────────────────
const TIME_SLOTS = [
  [6,  9,  'morning_rush'],
  [10, 15, 'midday'],
  [16, 19, 'evening_rush'],
  [20, 22, 'evening'],
];
const DAY_NAMES   = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
const PAYMENT_MAP = { '1':'credit_card', '2':'cash', '3':'no_charge', '4':'dispute' };

function transformRow(r) {
  const dist  = parseFloat(r['trip_distance']       || '0');
  const fare  = parseFloat(r['fare_amount']          || '0');
  const tip   = parseFloat(r['tip_amount']           || '0');
  const total = parseFloat(r['total_amount']         || '0');
  const pax   = parseInt  (r['passenger_count']      || '0');
  const pu    = parseInt  (r['PULocationID']         || '0');
  const doo   = parseInt  (r['DOLocationID']         || '0');
  const pt    = parseInt  (r['payment_type']         || '0');
  const vid   = parseInt  (r['VendorID']             || '0');
  const rid   = parseInt  (r['RatecodeID']           || '0');

  const pickup  = new Date((r['tpep_pickup_datetime']  || '').replace(' ', 'T'));
  const dropoff = new Date((r['tpep_dropoff_datetime'] || '').replace(' ', 'T'));

  // ── Validity gates ──
  if (isNaN(pickup) || isNaN(dropoff))          return null;
  if (dist  <= 0   || dist  > 200)              return null;
  if (fare  <= 0   || fare  > 500)              return null;
  if (total <= 0)                               return null;
  if (tip   <  0)                               return null;
  if (pax   <= 0   || pax   > 9)               return null;

  const durMin   = (dropoff - pickup) / 60000;
  if (durMin <= 0 || durMin > 300)              return null;

  // Reject physically impossible speeds (> 120 mph for NYC taxi)
  const speed = dist / (durMin / 60);
  if (speed > 120)                              return null;

  const hour    = pickup.getHours();
  const dow     = pickup.getDay();
  const slot    = TIME_SLOTS.find(([a, b]) => hour >= a && hour <= b);
  const timeSlot= slot ? slot[2] : 'overnight';

  return {
    // identity
    vendor_id:              vid,
    // timestamps
    pickup_datetime:        r['tpep_pickup_datetime'],
    dropoff_datetime:       r['tpep_dropoff_datetime'],
    pickup_date:            r['tpep_pickup_datetime'].slice(0, 10),
    pickup_year:            pickup.getFullYear(),
    pickup_month:           pickup.getMonth() + 1,
    pickup_day:             pickup.getDate(),
    pickup_hour:            hour,
    day_of_week:            dow,
    day_name:               DAY_NAMES[dow],
    is_weekend:             dow === 0 || dow === 6,
    // locations
    pu_location_id:         pu,
    do_location_id:         doo,
    rate_code_id:           rid,
    store_and_fwd_flag:     r['store_and_fwd_flag'] || 'N',
    // passengers
    passenger_count:        pax,
    // financials
    fare_amount:            fare,
    extra:                  parseFloat(r['extra']                  || '0'),
    mta_tax:                parseFloat(r['mta_tax']                || '0'),
    tip_amount:             tip,
    tolls_amount:           parseFloat(r['tolls_amount']           || '0'),
    improvement_surcharge:  parseFloat(r['improvement_surcharge']  || '0'),
    congestion_surcharge:   parseFloat(r['congestion_surcharge']   || '0'),
    total_amount:           total,
    payment_type:           pt,
    payment_type_label:     PAYMENT_MAP[String(pt)] || 'unknown',
    // derived
    trip_distance:          dist,
    trip_duration_minutes:  +durMin.toFixed(2),
    avg_speed_mph:          +(dist / (durMin / 60)).toFixed(2),
    time_of_day:            timeSlot,
    was_tipped:             tip > 0,
    tip_pct:                fare > 0 ? +(tip / fare * 100).toFixed(2) : 0,
    fare_per_mile:          dist > 0 ? +(fare / dist).toFixed(2) : 0,
    revenue_per_minute:     durMin > 0 ? +(total / durMin).toFixed(2) : 0,
    is_long_trip:           dist >= 10,
    is_airport_trip:        [132, 138].includes(pu) || [132, 138].includes(doo),
  };
}

// ── Gold aggregation ──────────────────────────────────────────────────────────
function buildGold(silver) {
  const byHour = new Map();

  for (const r of silver) {
    const key = `${r.pickup_date}|${r.pickup_hour}|${r.pu_location_id}`;
    if (!byHour.has(key)) byHour.set(key, {
      pickup_date:     r.pickup_date,
      pickup_year:     r.pickup_year,
      pickup_month:    r.pickup_month,
      pickup_hour:     r.pickup_hour,
      day_of_week:     r.day_of_week,
      day_name:        r.day_name,
      is_weekend:      r.is_weekend,
      time_of_day:     r.time_of_day,
      pu_location_id:  r.pu_location_id,
      n: 0, rev: 0, dist: 0, pax: 0, tip: 0, dur: 0,
      tipped: 0, long: 0, airport: 0,
    });

    const b = byHour.get(key);
    b.n       += 1;
    b.rev     += r.total_amount;
    b.dist    += r.trip_distance;
    b.pax     += r.passenger_count;
    b.tip     += r.tip_amount;
    b.dur     += r.trip_duration_minutes;
    if (r.was_tipped)    b.tipped++;
    if (r.is_long_trip)  b.long++;
    if (r.is_airport_trip) b.airport++;
  }

  return [...byHour.values()].map(b => ({
    pickup_date:          b.pickup_date,
    pickup_year:          b.pickup_year,
    pickup_month:         b.pickup_month,
    pickup_hour:          b.pickup_hour,
    day_of_week:          b.day_of_week,
    day_name:             b.day_name,
    is_weekend:           b.is_weekend,
    time_of_day:          b.time_of_day,
    pu_location_id:       b.pu_location_id,
    trip_count:           b.n,
    total_revenue:        +b.rev.toFixed(2),
    avg_fare:             +(b.rev / b.n).toFixed(2),
    avg_distance_miles:   +(b.dist / b.n).toFixed(2),
    avg_duration_minutes: +(b.dur / b.n).toFixed(2),
    avg_tip:              +(b.tip / b.n).toFixed(2),
    tip_rate_pct:         +(b.tipped / b.n * 100).toFixed(1),
    long_trip_pct:        +(b.long / b.n * 100).toFixed(1),
    airport_trip_pct:     +(b.airport / b.n * 100).toFixed(1),
    total_passengers:     b.pax,
    revenue_per_mile:     b.dist > 0 ? +(b.rev / b.dist).toFixed(2) : 0,
  })).sort((a, b) =>
    a.pickup_date.localeCompare(b.pickup_date) || a.pickup_hour - b.pickup_hour
  );
}

// ── Parquet writer ────────────────────────────────────────────────────────────
async function writeParquet(rows, outPath, schema) {
  const search = [
    path.join(__dirname, '..', 'node_modules', 'parquetjs-lite'),
    '/tmp/node_modules/parquetjs-lite',
    path.join(process.cwd(), 'node_modules', 'parquetjs-lite'),
  ];

  let parquet = null;
  for (const p of search) {
    try { parquet = require(p); break; } catch (_) {}
  }

  if (!parquet) {
    // JSONL fallback — still Iceberg-compatible (Spark can read JSONL)
    const jsonlPath = outPath.replace(/\.parquet$/, '.jsonl');
    warn(`parquetjs-lite not found → writing JSONL: ${jsonlPath}`);
    warn('  To get real Parquet: npm install parquetjs-lite (in project root)');
    fs.writeFileSync(jsonlPath, rows.map(r => JSON.stringify(r)).join('\n'), 'utf8');
    return { path: jsonlPath, format: 'jsonl', rows: rows.length, size: fs.statSync(jsonlPath).size };
  }

  const writer = await parquet.ParquetWriter.openFile(
    new parquet.ParquetSchema(schema), outPath
  );
  for (const row of rows) await writer.appendRow(row);
  await writer.close();

  return { path: outPath, format: 'parquet', rows: rows.length, size: fs.statSync(outPath).size };
}

// ── AWS Signature V4 ──────────────────────────────────────────────────────────
function awsSign(method, urlStr, body, contentType = 'application/octet-stream') {
  const url      = new URL(urlStr);
  const now      = new Date();
  const amzDate  = now.toISOString().replace(/[:\-]|\.\d{3}/g, '').slice(0, 15) + 'Z';
  const dateStmp = amzDate.slice(0, 8);
  const bodyHash = crypto.createHash('sha256').update(body).digest('hex');

  const hdr = {
    'host':                  url.host,
    'x-amz-date':           amzDate,
    'x-amz-content-sha256': bodyHash,
    'content-type':         contentType,
    'content-length':       String(body.length),
  };

  const signedHdr = Object.keys(hdr).sort().join(';');
  const canonHdr  = Object.keys(hdr).sort().map(k => `${k}:${hdr[k]}\n`).join('');

  const canonical = [method, url.pathname, '', canonHdr, signedHdr, bodyHash].join('\n');
  const scope     = `${dateStmp}/${REGION}/s3/aws4_request`;
  const sts       = ['AWS4-HMAC-SHA256', amzDate, scope,
                     crypto.createHash('sha256').update(canonical).digest('hex')].join('\n');

  const kDate    = crypto.createHmac('sha256', 'AWS4' + SECRET_KEY).update(dateStmp).digest();
  const kRegion  = crypto.createHmac('sha256', kDate).update(REGION).digest();
  const kService = crypto.createHmac('sha256', kRegion).update('s3').digest();
  const kSign    = crypto.createHmac('sha256', kService).update('aws4_request').digest();
  const sig      = crypto.createHmac('sha256', kSign).update(sts).digest('hex');

  return {
    ...hdr,
    Authorization: `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${scope}, SignedHeaders=${signedHdr}, Signature=${sig}`,
  };
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────
function httpReq(method, urlStr, hdrs, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.request({
      hostname: url.hostname, port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + (url.search || ''), method, headers: hdrs,
    }, res => {
      let out = '';
      res.on('data', d => out += d);
      res.on('end', () => resolve({ status: res.statusCode, body: out, headers: res.headers }));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function minioHealthy() {
  try {
    const r = await httpReq('GET', `${MINIO_ENDPOINT}/minio/health/live`, {}, null);
    return r.status === 200;
  } catch (_) { return false; }
}

async function waitForMinio(maxMs = 60000) {
  const start = Date.now();
  let attempt = 0;
  while (Date.now() - start < maxMs) {
    if (await minioHealthy()) return true;
    attempt++;
    log(`MinIO not ready (attempt ${attempt}) — retrying in 3s…`);
    await new Promise(r => setTimeout(r, 3000));
  }
  return false;
}

async function minioCreateBucket(bucket) {
  const url = `${MINIO_ENDPOINT}/${bucket}`;
  const hdrs = awsSign('PUT', url, Buffer.alloc(0), 'application/xml');
  const r = await httpReq('PUT', url, hdrs, Buffer.alloc(0));
  if (r.status === 200 || r.status === 409) return; // 409 = already exists
  throw new Error(`Create bucket ${bucket}: HTTP ${r.status} ${r.body.slice(0, 200)}`);
}

async function minioUpload(localPath, bucket, s3Key, contentType) {
  const body = fs.readFileSync(localPath);
  const url  = `${MINIO_ENDPOINT}/${bucket}/${s3Key}`;
  const hdrs = awsSign('PUT', url, body, contentType);
  const r    = await httpReq('PUT', url, hdrs, body);
  if (r.status < 200 || r.status >= 300)
    throw new Error(`Upload ${s3Key}: HTTP ${r.status}\n${r.body.slice(0, 300)}`);
  return { size: body.length, etag: r.headers.etag || '' };
}

// ── Iceberg-style metadata manifest ──────────────────────────────────────────
function buildIcebergManifest(tableName, fileInfo, partitionKey, partitionVal) {
  return {
    'format-version': 2,
    'table-uuid':     crypto.randomUUID(),
    'location':       `s3://${BUCKETS.silver}/${tableName}`,
    'metadata-log':   [],
    'schemas': [{
      'schema-id':   0,
      'type':        'struct',
      'fields':      Object.keys(fileInfo.schema || {}).map((name, id) => ({
        id, name, required: false, type: 'string',
      })),
    }],
    'current-schema-id': 0,
    'partition-specs': [{
      'spec-id': 0,
      'fields':  [{ 'field-id': 1000, name: partitionKey, 'source-id': 0, transform: 'identity' }],
    }],
    'current-spec-id': 0,
    'snapshots': [{
      'snapshot-id':    Date.now(),
      'timestamp-ms':   Date.now(),
      'summary': {
        operation:          'append',
        'added-data-files': '1',
        'added-records':    String(fileInfo.rows),
        'added-files-size': String(fileInfo.size),
      },
      'manifest-list': `s3://${BUCKETS.silver}/${tableName}/metadata/snap-${Date.now()}.avro`,
    }],
    'current-snapshot-id': Date.now(),
    'refs': { main: { type: 'branch', 'snapshot-id': Date.now() } },
    'statistics':   [],
    'properties': {
      'write.format.default':     fileInfo.format,
      'write.parquet.compression': 'snappy',
      'partition.column':          partitionKey,
      'partition.value':           partitionVal,
      'created-at':                new Date().toISOString(),
    },
  };
}

// ── Data Quality Report ───────────────────────────────────────────────────────
function dqReport(silver) {
  const rules = [
    ['trip_distance ∈ (0, 200]',   silver.every(r => r.trip_distance > 0 && r.trip_distance <= 200)],
    ['fare_amount ∈ (0, 500]',     silver.every(r => r.fare_amount > 0 && r.fare_amount <= 500)],
    ['total_amount > 0',           silver.every(r => r.total_amount > 0)],
    ['passenger_count ∈ [1, 9]',   silver.every(r => r.passenger_count >= 1 && r.passenger_count <= 9)],
    ['trip_duration ∈ (0, 300] min', silver.every(r => r.trip_duration_minutes > 0 && r.trip_duration_minutes <= 300)],
    ['avg_speed_mph ∈ (0, 120)',   silver.every(r => r.avg_speed_mph > 0 && r.avg_speed_mph < 120)],
    ['valid time_of_day values',   silver.every(r => ['morning_rush','midday','evening_rush','evening','overnight'].includes(r.time_of_day))],
    ['tip_pct >= 0',               silver.every(r => r.tip_pct >= 0)],
    ['pickup_date populated',      silver.every(r => /^\d{4}-\d{2}-\d{2}$/.test(r.pickup_date))],
    ['no null payment_type_label', silver.every(r => r.payment_type_label)],
  ];

  let passed = 0;
  for (const [name, result] of rules) {
    result ? ok(`DQ ✓  ${name}`) : fail(`DQ ✗  ${name}`);
    if (result) passed++;
  }
  return { total: rules.length, passed, failed: rules.length - passed };
}

// ── Pretty table ──────────────────────────────────────────────────────────────
function printTable(rows, cols) {
  const widths = cols.map(c => Math.max(c.label.length, ...rows.map(r => String(r[c.key] ?? '').length)));
  const sep = '  ' + widths.map(w => '─'.repeat(w + 2)).join('┼') + '  ';
  const row = (r) => '  ' + cols.map((c, i) =>
    ` ${String(r[c.key] ?? '').padEnd(widths[i])} `
  ).join('│') + '  ';

  console.log('\n' + '  ' + cols.map((c, i) => ` ${c.label.padEnd(widths[i])} `).join('│'));
  console.log(sep);
  rows.forEach(r => console.log(row(r)));
  console.log('');
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════════════════════════════

async function main() {
  const T0 = Date.now();

  console.log(`\n${C.bold}${C.cyan}`);
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║         E N D - T O - E N D   E T L   P I P E L I N E    ║');
  console.log('║  CSV → MinIO (raw) → Transform → Parquet → MinIO (silver) ║');
  console.log('║  → Gold Aggregation → MinIO (gold) → Iceberg Manifest     ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(C.reset);

  if (DRY_RUN) warn('DRY-RUN mode — all MinIO uploads will be skipped');

  const stats = { passed: 0, failed: 0, warnings: 0 };
  const track = (ok, msg) => { ok ? stats.passed++ : stats.failed++; ok ? ok(msg) : fail(msg); };

  // ──────────────────────────────────────────────────────────────────────────
  // Stage 1  PREPARE CSV
  // ──────────────────────────────────────────────────────────────────────────
  head(1, 'Prepare source CSV');

  let csvPath = CSV_PATH;

  if (GENERATE || !fs.existsSync(csvPath)) {
    log('Generating 2 000-row synthetic NYC taxi dataset…');
    const csv = generateCSV(2000);
    csvPath   = '/tmp/etl_nyc_taxi_input.csv';
    fs.writeFileSync(csvPath, csv, 'utf8');
    ok(`Wrote synthetic CSV → ${csvPath}`);
  } else {
    ok(`Source CSV: ${csvPath}`);
  }

  const csvContent = fs.readFileSync(csvPath, 'utf8');
  const rawRows    = parseCSV(csvContent);
  ok(`Parsed ${rawRows.length.toLocaleString()} raw rows  (${fmtBytes(Buffer.byteLength(csvContent, 'utf8'))})`);

  // ──────────────────────────────────────────────────────────────────────────
  // Stage 2  UPLOAD RAW CSV → MinIO
  // ──────────────────────────────────────────────────────────────────────────
  head(2, `Upload raw CSV → MinIO  [${BUCKETS.raw}]`);

  let minioOk = false;

  if (DRY_RUN) {
    warn('Skipping MinIO (--dry-run)');
    stats.warnings++;
  } else {
    if (WAIT_MINIO) {
      log(`Waiting for MinIO at ${MINIO_ENDPOINT} (up to 60 s)…`);
      minioOk = await waitForMinio(60000);
    } else {
      minioOk = await minioHealthy();
    }

    if (!minioOk) {
      warn(`MinIO unreachable at ${MINIO_ENDPOINT}`);
      warn('Start with:  docker compose up -d minio && docker compose up minio-init');
      warn('Or re-run with --wait to poll until ready');
      stats.warnings += 3;
    } else {
      ok(`MinIO healthy: ${MINIO_ENDPOINT}`);

      // Ensure buckets exist
      for (const [name, bkt] of Object.entries(BUCKETS)) {
        try {
          await minioCreateBucket(bkt);
          ok(`Bucket ready: s3://${bkt}`);
        } catch (e) {
          fail(`Bucket ${bkt}: ${e.message}`);
          stats.failed++;
        }
      }

      // Hive-partition path: year=YYYY/month=MM/day=DD/file.csv
      const now   = new Date();
      const yy    = now.getFullYear();
      const mm    = String(now.getMonth() + 1).padStart(2, '0');
      const dd    = String(now.getDate()).padStart(2, '0');
      const fname = path.basename(csvPath);
      const rawKey = `nyc_taxi/year=${yy}/month=${mm}/day=${dd}/${fname}`;

      try {
        const r = await minioUpload(csvPath, BUCKETS.raw, rawKey, 'text/csv');
        ok(`Uploaded raw CSV → s3://${BUCKETS.raw}/${rawKey}  (${fmtBytes(r.size)})`);
        stats.passed++;
      } catch (e) {
        fail(`Raw upload failed: ${e.message.slice(0, 200)}`);
        stats.failed++;
      }
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Stage 3  TRANSFORM → Silver
  // ──────────────────────────────────────────────────────────────────────────
  head(3, 'Transform raw → silver (typing, DQ, feature engineering)');

  const silver   = [];
  let   rejected = 0;

  for (const r of rawRows) {
    const s = transformRow(r);
    if (s) silver.push(s); else rejected++;
  }

  ok(`Silver rows: ${silver.length.toLocaleString()}  |  Rejected: ${rejected.toLocaleString()} (${(rejected / rawRows.length * 100).toFixed(1)} %)`);
  log(`Features added: avg_speed_mph, time_of_day, tip_pct, fare_per_mile, is_long_trip, is_airport_trip, …`);

  // ──────────────────────────────────────────────────────────────────────────
  // Stage 4  DATA QUALITY
  // ──────────────────────────────────────────────────────────────────────────
  head(4, 'Data Quality checks (10 rules)');

  const dq = dqReport(silver);
  ok(`DQ summary: ${dq.passed}/${dq.total} rules passed`);
  if (dq.failed > 0) {
    fail(`${dq.failed} DQ rule(s) FAILED — aborting pipeline`);
    process.exit(1);
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Stage 5  WRITE + UPLOAD SILVER PARQUET
  // ──────────────────────────────────────────────────────────────────────────
  head(5, `Write Parquet + upload → MinIO  [${BUCKETS.silver}]`);

  const silverSchema = {
    vendor_id:              { type: 'INT32' },
    pickup_datetime:        { type: 'UTF8' },
    dropoff_datetime:       { type: 'UTF8' },
    pickup_date:            { type: 'UTF8' },
    pickup_year:            { type: 'INT32' },
    pickup_month:           { type: 'INT32' },
    pickup_day:             { type: 'INT32' },
    pickup_hour:            { type: 'INT32' },
    day_of_week:            { type: 'INT32' },
    day_name:               { type: 'UTF8' },
    is_weekend:             { type: 'BOOLEAN' },
    pu_location_id:         { type: 'INT32' },
    do_location_id:         { type: 'INT32' },
    rate_code_id:           { type: 'INT32' },
    store_and_fwd_flag:     { type: 'UTF8' },
    passenger_count:        { type: 'INT32' },
    fare_amount:            { type: 'DOUBLE' },
    extra:                  { type: 'DOUBLE' },
    mta_tax:                { type: 'DOUBLE' },
    tip_amount:             { type: 'DOUBLE' },
    tolls_amount:           { type: 'DOUBLE' },
    improvement_surcharge:  { type: 'DOUBLE' },
    congestion_surcharge:   { type: 'DOUBLE' },
    total_amount:           { type: 'DOUBLE' },
    payment_type:           { type: 'INT32' },
    payment_type_label:     { type: 'UTF8' },
    trip_distance:          { type: 'DOUBLE' },
    trip_duration_minutes:  { type: 'DOUBLE' },
    avg_speed_mph:          { type: 'DOUBLE' },
    time_of_day:            { type: 'UTF8' },
    was_tipped:             { type: 'BOOLEAN' },
    tip_pct:                { type: 'DOUBLE' },
    fare_per_mile:          { type: 'DOUBLE' },
    revenue_per_minute:     { type: 'DOUBLE' },
    is_long_trip:           { type: 'BOOLEAN' },
    is_airport_trip:        { type: 'BOOLEAN' },
  };

  const silverOut = await writeParquet(silver, '/tmp/etl_silver_trips.parquet', silverSchema);
  ok(`Silver file: ${silverOut.path}  (${silverOut.format.toUpperCase()}, ${silverOut.rows.toLocaleString()} rows, ${fmtBytes(silverOut.size)})`);

  if (!DRY_RUN && minioOk) {
    const ext     = silverOut.format === 'parquet' ? 'parquet' : 'jsonl';
    const ct      = silverOut.format === 'parquet' ? 'application/octet-stream' : 'application/x-ndjson';
    const silverKey = `silver/trips/pickup_year=2024/pickup_month=01/part-00000.${ext}`;

    try {
      const r = await minioUpload(silverOut.path, BUCKETS.silver, silverKey, ct);
      ok(`Uploaded silver → s3://${BUCKETS.silver}/${silverKey}  (${fmtBytes(r.size)})`);
      stats.passed++;
    } catch (e) {
      fail(`Silver upload: ${e.message.slice(0, 200)}`);
      stats.failed++;
    }

    // Iceberg-style metadata manifest
    const manifest = buildIcebergManifest('trips', { ...silverOut, schema: silverSchema }, 'pickup_month', '01');
    const manifestJson = JSON.stringify(manifest, null, 2);
    const manifestPath = '/tmp/etl_iceberg_manifest.json';
    fs.writeFileSync(manifestPath, manifestJson, 'utf8');
    const manifestKey = 'silver/trips/metadata/latest.json';

    try {
      await minioUpload(manifestPath, BUCKETS.silver, manifestKey, 'application/json');
      ok(`Iceberg manifest → s3://${BUCKETS.silver}/${manifestKey}`);
      stats.passed++;
    } catch (e) {
      warn(`Manifest upload: ${e.message.slice(0, 100)}`);
      stats.warnings++;
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Stage 6  GOLD AGGREGATION
  // ──────────────────────────────────────────────────────────────────────────
  head(6, 'Gold aggregation (hourly KPIs per date + pickup zone)');

  const gold     = buildGold(silver);
  const days     = new Set(gold.map(r => r.pickup_date)).size;
  const totRev   = gold.reduce((s, r) => s + r.total_revenue, 0);
  const totTrips = gold.reduce((s, r) => s + r.trip_count, 0);

  ok(`Gold rows: ${gold.length.toLocaleString()}  (${days} days, ${gold.length} hourly-zone buckets)`);
  ok(`Total trips:   ${totTrips.toLocaleString()}`);
  ok(`Total revenue: $${totRev.toFixed(2)}`);
  ok(`Avg fare:      $${(totRev / totTrips).toFixed(2)}`);

  // ──────────────────────────────────────────────────────────────────────────
  // Stage 7  WRITE + UPLOAD GOLD
  // ──────────────────────────────────────────────────────────────────────────
  head(7, `Write gold Parquet + upload → MinIO  [${BUCKETS.gold}]`);

  const goldSchema = {
    pickup_date:          { type: 'UTF8' },
    pickup_year:          { type: 'INT32' },
    pickup_month:         { type: 'INT32' },
    pickup_hour:          { type: 'INT32' },
    day_of_week:          { type: 'INT32' },
    day_name:             { type: 'UTF8' },
    is_weekend:           { type: 'BOOLEAN' },
    time_of_day:          { type: 'UTF8' },
    pu_location_id:       { type: 'INT32' },
    trip_count:           { type: 'INT32' },
    total_revenue:        { type: 'DOUBLE' },
    avg_fare:             { type: 'DOUBLE' },
    avg_distance_miles:   { type: 'DOUBLE' },
    avg_duration_minutes: { type: 'DOUBLE' },
    avg_tip:              { type: 'DOUBLE' },
    tip_rate_pct:         { type: 'DOUBLE' },
    long_trip_pct:        { type: 'DOUBLE' },
    airport_trip_pct:     { type: 'DOUBLE' },
    total_passengers:     { type: 'INT32' },
    revenue_per_mile:     { type: 'DOUBLE' },
  };

  const goldOut = await writeParquet(gold, '/tmp/etl_gold_summary.parquet', goldSchema);
  ok(`Gold file: ${goldOut.path}  (${goldOut.format.toUpperCase()}, ${goldOut.rows.toLocaleString()} rows, ${fmtBytes(goldOut.size)})`);

  if (!DRY_RUN && minioOk) {
    const ext    = goldOut.format === 'parquet' ? 'parquet' : 'jsonl';
    const ct     = goldOut.format === 'parquet' ? 'application/octet-stream' : 'application/x-ndjson';
    const goldKey = `gold/hourly_zone_summary/pickup_year=2024/pickup_month=01/part-00000.${ext}`;

    try {
      const r = await minioUpload(goldOut.path, BUCKETS.gold, goldKey, ct);
      ok(`Uploaded gold  → s3://${BUCKETS.gold}/${goldKey}  (${fmtBytes(r.size)})`);
      stats.passed++;
    } catch (e) {
      fail(`Gold upload: ${e.message.slice(0, 200)}`);
      stats.failed++;
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // SUMMARY
  // ──────────────────────────────────────────────────────────────────────────
  const elapsed = ((Date.now() - T0) / 1000).toFixed(2);

  console.log(`\n${C.bold}${C.cyan}`);
  console.log('╔════════════════════════════════════════════════╗');
  console.log('║               P I P E L I N E   S U M M A R Y ║');
  console.log('╚════════════════════════════════════════════════╝');
  console.log(C.reset);

  printTable([
    { metric: 'Raw rows read',      value: rawRows.length.toLocaleString() },
    { metric: 'Silver rows',        value: silver.length.toLocaleString() },
    { metric: 'Rejected (DQ)',      value: rejected.toLocaleString() },
    { metric: 'Gold buckets',       value: gold.length.toLocaleString() },
    { metric: 'Total revenue',      value: `$${totRev.toFixed(2)}` },
    { metric: 'Avg fare',           value: `$${(totRev / totTrips).toFixed(2)}` },
    { metric: 'Silver file',        value: `${silverOut.format.toUpperCase()}  ${fmtBytes(silverOut.size)}` },
    { metric: 'Gold file',          value: `${goldOut.format.toUpperCase()}  ${fmtBytes(goldOut.size)}` },
    { metric: 'Pipeline duration',  value: `${elapsed} s` },
    { metric: 'MinIO connected',    value: minioOk ? '✓ yes' : '✗ no (uploads skipped)' },
  ], [
    { key: 'metric', label: 'Metric' },
    { key: 'value',  label: 'Value' },
  ]);

  // Top-5 revenue hours
  console.log(`${C.bold}  Top 5 hours by revenue:${C.reset}`);
  printTable(
    [...gold].sort((a, b) => b.total_revenue - a.total_revenue).slice(0, 5).map(r => ({
      date:    r.pickup_date,
      hour:    `${r.pickup_hour}:00`,
      period:  r.time_of_day,
      trips:   r.trip_count,
      revenue: `$${r.total_revenue.toFixed(2)}`,
      avg:     `$${r.avg_fare.toFixed(2)}`,
      tip_rt:  `${r.tip_rate_pct}%`,
    })),
    [
      { key: 'date',    label: 'Date' },
      { key: 'hour',    label: 'Hour' },
      { key: 'period',  label: 'Period' },
      { key: 'trips',   label: 'Trips' },
      { key: 'revenue', label: 'Revenue' },
      { key: 'avg',     label: 'Avg Fare' },
      { key: 'tip_rt',  label: 'Tip Rate' },
    ]
  );

  if (stats.failed === 0) {
    console.log(`${C.bold}${C.green}✅  Pipeline complete  (${elapsed} s)${C.reset}`);
    if (!minioOk && !DRY_RUN) {
      console.log(`\n${C.yellow}  MinIO was offline — files written locally:${C.reset}`);
      console.log(`  Silver: ${silverOut.path}`);
      console.log(`  Gold:   ${goldOut.path}`);
      console.log(`\n  To also upload: docker compose up -d minio && node scripts/etl_pipeline.js --wait`);
    }
  } else {
    console.log(`${C.bold}${C.red}❌  Pipeline failed  (${stats.failed} error(s))${C.reset}`);
    process.exit(1);
  }
}

main().catch(e => {
  console.error(`\n${C.red}${C.bold}FATAL: ${e.message}${C.reset}`);
  console.error(e.stack);
  process.exit(1);
});
