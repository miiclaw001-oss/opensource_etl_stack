#!/usr/bin/env node
/**
 * local_etl_demo.js — End-to-End ETL Demo (no Spark required)
 *
 * Pipeline:
 *   Local CSV  →  MinIO (raw)  →  Transform (Node.js)  →  Parquet (silver)  →  MinIO (silver)
 *
 * Usage:
 *   node scripts/local_etl_demo.js [--csv /path/to/file.csv] [--minio http://localhost:9000]
 *   node scripts/local_etl_demo.js --generate   # generate + run full pipeline
 *
 * Requirements:
 *   npm install parquetjs-lite  (in project root or /tmp)
 *
 * Environment:
 *   MINIO_ENDPOINT    default: http://localhost:9000
 *   MINIO_ACCESS_KEY  default: minioadmin
 *   MINIO_SECRET_KEY  default: minioadmin123
 */

'use strict';

const fs     = require('fs');
const path   = require('path');
const https  = require('https');
const http   = require('http');
const crypto = require('crypto');
const os     = require('os');

// ── Config ────────────────────────────────────────────────────────────────────
const MINIO_ENDPOINT  = process.env.MINIO_ENDPOINT  || 'http://localhost:9000';
const ACCESS_KEY      = process.env.MINIO_ACCESS_KEY || 'minioadmin';
const SECRET_KEY      = process.env.MINIO_SECRET_KEY || 'minioadmin123';
const REGION          = process.env.AWS_REGION       || 'us-east-1';

const RAW_BUCKET      = 'warehouse-raw';
const SILVER_BUCKET   = 'warehouse-silver';

// ── CLI args ──────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const GENERATE_DATA   = args.includes('--generate');
const CSV_PATH        = (() => {
  const i = args.indexOf('--csv');
  return i >= 0 ? args[i + 1] : '/tmp/etl_test_data.csv';
})();

// ── Colours ───────────────────────────────────────────────────────────────────
const C = {
  reset:  '\x1b[0m',
  bold:   '\x1b[1m',
  green:  '\x1b[32m',
  cyan:   '\x1b[36m',
  yellow: '\x1b[33m',
  red:    '\x1b[31m',
  blue:   '\x1b[34m',
};
const log   = (m) => console.log(`${C.blue}[ETL]${C.reset} ${m}`);
const ok    = (m) => console.log(`${C.green}  ✓${C.reset} ${m}`);
const warn  = (m) => console.log(`${C.yellow}  ⚠${C.reset} ${m}`);
const err   = (m) => console.error(`${C.red}  ✗${C.reset} ${m}`);
const step  = (m) => console.log(`\n${C.bold}${C.cyan}── ${m} ──${C.reset}`);

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Parse a CSV string into array of objects */
function parseCSV(text) {
  const lines = text.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim());
  return lines.slice(1).map(line => {
    const vals = line.split(',');
    const row = {};
    headers.forEach((h, i) => { row[h] = (vals[i] || '').trim(); });
    return row;
  });
}

/** Generate synthetic NYC taxi CSV data */
function generateSyntheticCSV(rows = 1000) {
  const headers = [
    'VendorID','tpep_pickup_datetime','tpep_dropoff_datetime',
    'passenger_count','trip_distance','RatecodeID','store_and_fwd_flag',
    'PULocationID','DOLocationID','payment_type','fare_amount','extra',
    'mta_tax','tip_amount','tolls_amount','improvement_surcharge',
    'total_amount','congestion_surcharge'
  ];

  const rand = (min, max) => Math.random() * (max - min) + min;
  const randInt = (min, max) => Math.floor(rand(min, max + 1));
  const fmt = (d) => {
    const pad = n => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ` +
           `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  };

  const csvRows = [headers.join(',')];
  const base = new Date('2024-01-01T00:00:00');

  for (let i = 0; i < rows; i++) {
    const pickup = new Date(base.getTime() + randInt(0, 27 * 86400) * 1000 + randInt(0, 86399) * 1000);
    const durationMin = randInt(3, 90);
    const dropoff = new Date(pickup.getTime() + durationMin * 60000);
    const dist = parseFloat(rand(0.5, 25).toFixed(2));
    const fare = parseFloat((2.5 + dist * 2.5 + rand(0, 5)).toFixed(2));
    const tip  = parseFloat((fare * rand(0, 0.3)).toFixed(2));
    const total = parseFloat((fare + tip + 1.3).toFixed(2));

    csvRows.push([
      randInt(1, 2),
      fmt(pickup),
      fmt(dropoff),
      randInt(1, 6),
      dist,
      1,
      'N',
      randInt(1, 265),
      randInt(1, 265),
      randInt(1, 4),
      fare,
      0.5,
      0.5,
      tip,
      0,
      0.3,
      total,
      2.5,
    ].join(','));
  }

  return csvRows.join('\n');
}

/** Transform raw row → silver row (business logic) */
function transformRow(raw) {
  const dist   = parseFloat(raw['trip_distance'] || '0');
  const fare   = parseFloat(raw['fare_amount']   || '0');
  const tip    = parseFloat(raw['tip_amount']    || '0');
  const total  = parseFloat(raw['total_amount']  || '0');

  const pickup  = new Date(raw['tpep_pickup_datetime'].replace(' ', 'T'));
  const dropoff = new Date(raw['tpep_dropoff_datetime'].replace(' ', 'T'));

  if (isNaN(pickup) || isNaN(dropoff)) return null;

  const durationMin = (dropoff - pickup) / 60000;

  // Business rules
  if (dist  <= 0 || dist  > 200)  return null;
  if (fare  <= 0 || fare  > 500)  return null;
  if (total <= 0)                  return null;
  if (durationMin <= 0 || durationMin > 300) return null;

  const hour = pickup.getHours();
  const dow  = pickup.getDay(); // 0=Sun

  const timeOfDay =
    hour >= 6  && hour <= 9  ? 'morning_rush' :
    hour >= 10 && hour <= 15 ? 'midday'        :
    hour >= 16 && hour <= 19 ? 'evening_rush'  :
    hour >= 20 && hour <= 22 ? 'evening'        : 'overnight';

  const dayName = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][dow];

  const paymentLabel =
    raw['payment_type'] === '1' ? 'credit_card' :
    raw['payment_type'] === '2' ? 'cash'         :
    raw['payment_type'] === '3' ? 'no_charge'    :
    raw['payment_type'] === '4' ? 'dispute'      : 'unknown';

  return {
    vendor_id:             parseInt(raw['VendorID'] || '0'),
    pickup_datetime:       raw['tpep_pickup_datetime'],
    dropoff_datetime:      raw['tpep_dropoff_datetime'],
    pickup_date:           raw['tpep_pickup_datetime'].slice(0, 10),
    pickup_hour:           hour,
    day_of_week:           dow,
    day_name:              dayName,
    is_weekend:            dow === 0 || dow === 6,
    passenger_count:       parseInt(raw['passenger_count'] || '0'),
    trip_distance:         dist,
    pu_location_id:        parseInt(raw['PULocationID'] || '0'),
    do_location_id:        parseInt(raw['DOLocationID'] || '0'),
    payment_type:          parseInt(raw['payment_type'] || '0'),
    payment_type_label:    paymentLabel,
    fare_amount:           fare,
    tip_amount:            tip,
    total_amount:          total,
    trip_duration_minutes: parseFloat(durationMin.toFixed(2)),
    avg_speed_mph:         durationMin > 0 ? parseFloat((dist / (durationMin / 60)).toFixed(2)) : 0,
    time_of_day:           timeOfDay,
    was_tipped:            tip > 0,
    tip_pct:               fare > 0 ? parseFloat(((tip / fare) * 100).toFixed(2)) : 0,
    fare_per_mile:         dist > 0 ? parseFloat((fare / dist).toFixed(2)) : 0,
    revenue_per_minute:    durationMin > 0 ? parseFloat((total / durationMin).toFixed(2)) : 0,
  };
}

/** Compute gold aggregation from silver rows */
function computeGoldSummary(silverRows) {
  const buckets = new Map();

  for (const row of silverRows) {
    const key = `${row.pickup_date}|${row.pickup_hour}|${row.time_of_day}`;
    if (!buckets.has(key)) {
      buckets.set(key, {
        pickup_date:      row.pickup_date,
        pickup_hour:      row.pickup_hour,
        time_of_day:      row.time_of_day,
        day_of_week:      row.day_of_week,
        day_name:         row.day_name,
        is_weekend:       row.is_weekend,
        trip_count:       0,
        total_revenue:    0,
        total_distance:   0,
        total_passengers: 0,
        total_tip:        0,
        total_duration:   0,
        tipped_trips:     0,
      });
    }
    const b = buckets.get(key);
    b.trip_count       += 1;
    b.total_revenue    += row.total_amount;
    b.total_distance   += row.trip_distance;
    b.total_passengers += row.passenger_count;
    b.total_tip        += row.tip_amount;
    b.total_duration   += row.trip_duration_minutes;
    if (row.was_tipped) b.tipped_trips += 1;
  }

  return Array.from(buckets.values()).map(b => ({
    pickup_date:           b.pickup_date,
    pickup_hour:           b.pickup_hour,
    time_of_day:           b.time_of_day,
    day_of_week:           b.day_of_week,
    day_name:              b.day_name,
    is_weekend:            b.is_weekend,
    trip_count:            b.trip_count,
    total_revenue:         parseFloat(b.total_revenue.toFixed(2)),
    avg_fare:              parseFloat((b.total_revenue / b.trip_count).toFixed(2)),
    avg_distance:          parseFloat((b.total_distance / b.trip_count).toFixed(2)),
    avg_duration_minutes:  parseFloat((b.total_duration / b.trip_count).toFixed(2)),
    avg_tip:               parseFloat((b.total_tip / b.trip_count).toFixed(2)),
    tip_rate_pct:          parseFloat(((b.tipped_trips / b.trip_count) * 100).toFixed(1)),
    total_passengers:      b.total_passengers,
    revenue_per_mile:      b.total_distance > 0
                             ? parseFloat((b.total_revenue / b.total_distance).toFixed(2))
                             : 0,
  })).sort((a, b) => a.pickup_date.localeCompare(b.pickup_date) || a.pickup_hour - b.pickup_hour);
}

// ── Parquet writer (simple, using parquetjs-lite) ─────────────────────────────

async function writeParquet(rows, outputPath, schema) {
  // Try parquetjs-lite first, fall back to JSON-in-Parquet-wrapper, then CSV
  const parquetPaths = [
    '/tmp/node_modules/parquetjs-lite',
    path.join(process.cwd(), 'node_modules/parquetjs-lite'),
    '/home/node/.openclaw/workspace/projects/opensource_etl_stack/node_modules/parquetjs-lite',
  ];

  let parquet = null;
  for (const p of parquetPaths) {
    try {
      parquet = require(p);
      break;
    } catch (_) {}
  }

  if (!parquet) {
    warn(`parquetjs-lite not found — writing JSON Lines (Iceberg-compatible) to ${outputPath}`);
    // Write JSONL as fallback — still Iceberg-readable via Spark/dbt
    const jsonl = rows.map(r => JSON.stringify(r)).join('\n');
    fs.writeFileSync(outputPath.replace('.parquet', '.jsonl'), jsonl, 'utf8');
    return outputPath.replace('.parquet', '.jsonl');
  }

  const writer = await parquet.ParquetWriter.openFile(
    new parquet.ParquetSchema(schema),
    outputPath
  );

  for (const row of rows) {
    await writer.appendRow(row);
  }
  await writer.close();
  return outputPath;
}

// ── AWS Signature V4 for MinIO ────────────────────────────────────────────────

function hmac(key, data) {
  return crypto.createHmac('sha256', key).update(data).digest();
}

function sha256(data) {
  return crypto.createHash('sha256').update(data).digest('hex');
}

function getSignatureKey(key, dateStamp, regionName, serviceName) {
  const kDate    = hmac('AWS4' + key, dateStamp);
  const kRegion  = hmac(kDate, regionName);
  const kService = hmac(kRegion, serviceName);
  const kSigning = hmac(kService, 'aws4_request');
  return kSigning;
}

/**
 * Upload a file to MinIO using AWS Signature V4
 */
async function uploadToMinIO(localPath, bucket, s3Key, contentType = 'application/octet-stream') {
  const url = new URL(`${MINIO_ENDPOINT}/${bucket}/${s3Key}`);
  const fileContent = fs.readFileSync(localPath);
  const fileSize    = fileContent.length;

  const now = new Date();
  const amzDate    = now.toISOString().replace(/[:\-]|\.\d{3}/g, '').slice(0, 15) + 'Z';
  const dateStamp  = amzDate.slice(0, 8);

  const payloadHash = sha256(fileContent);

  const headers = {
    'host':                 url.host,
    'x-amz-date':          amzDate,
    'x-amz-content-sha256': payloadHash,
    'content-type':        contentType,
    'content-length':      String(fileSize),
  };

  // Canonical request
  const signedHeaders = Object.keys(headers).sort().join(';');
  const canonicalHeaders = Object.keys(headers).sort()
    .map(k => `${k}:${headers[k]}\n`).join('');

  const canonicalRequest = [
    'PUT',
    url.pathname,
    '',
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join('\n');

  // String to sign
  const credentialScope = `${dateStamp}/${REGION}/s3/aws4_request`;
  const stringToSign = [
    'AWS4-HMAC-SHA256',
    amzDate,
    credentialScope,
    sha256(canonicalRequest),
  ].join('\n');

  // Signature
  const signingKey = getSignatureKey(SECRET_KEY, dateStamp, REGION, 's3');
  const signature  = crypto.createHmac('sha256', signingKey).update(stringToSign).digest('hex');

  const authHeader = [
    `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${credentialScope}`,
    `SignedHeaders=${signedHeaders}`,
    `Signature=${signature}`,
  ].join(', ');

  return new Promise((resolve, reject) => {
    const options = {
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname,
      method:   'PUT',
      headers: {
        ...headers,
        'Authorization': authHeader,
      },
    };

    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.request(options, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve({ statusCode: res.statusCode, size: fileSize });
        } else {
          reject(new Error(`MinIO upload failed: HTTP ${res.statusCode}\n${body}`));
        }
      });
    });

    req.on('error', reject);
    req.write(fileContent);
    req.end();
  });
}

/** Check if MinIO is reachable */
async function checkMinIO() {
  return new Promise((resolve) => {
    const url = new URL(`${MINIO_ENDPOINT}/minio/health/live`);
    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.get(url.toString(), { timeout: 3000 }, (res) => {
      resolve(res.statusCode === 200);
    });
    req.on('error', () => resolve(false));
    req.on('timeout', () => { req.destroy(); resolve(false); });
  });
}

// ── Main pipeline ─────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n${C.bold}${C.cyan}`);
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║   Local CSV → MinIO → Transform → Parquet/JSONL  ║');
  console.log('║   End-to-End ETL Demo                            ║');
  console.log('╚══════════════════════════════════════════════════╝');
  console.log(C.reset);

  const results = { passed: 0, failed: 0, skipped: 0 };
  const pass  = (m) => { ok(m);   results.passed++;  };
  const fail  = (m) => { err(m);  results.failed++;  };
  const skip  = (m) => { warn(m); results.skipped++; };

  // ── Step 1: Prepare CSV ───────────────────────────────────────────────────
  step('Step 1: Prepare Source CSV');

  let csvPath = CSV_PATH;

  if (GENERATE_DATA || !fs.existsSync(csvPath)) {
    log('Generating synthetic NYC taxi data (1000 rows)...');
    const csv = generateSyntheticCSV(1000);
    csvPath = '/tmp/nyc_taxi_demo.csv';
    fs.writeFileSync(csvPath, csv, 'utf8');
    pass(`Generated synthetic CSV: ${csvPath}`);
  } else {
    pass(`Using existing CSV: ${csvPath}`);
  }

  const csvContent = fs.readFileSync(csvPath, 'utf8');
  const rawRows    = parseCSV(csvContent);
  pass(`Parsed ${rawRows.length} raw rows from CSV`);

  // ── Step 2: Upload raw CSV to MinIO ───────────────────────────────────────
  step('Step 2: Upload Raw CSV → MinIO');

  const minioUp = await checkMinIO();
  if (!minioUp) {
    skip(`MinIO not reachable at ${MINIO_ENDPOINT} — skipping upload steps`);
    skip('To enable uploads: docker compose up -d minio && docker compose up minio-init');
  } else {
    pass(`MinIO reachable at ${MINIO_ENDPOINT}`);

    const today = new Date();
    const s3Key = `nyc_taxi/year=${today.getFullYear()}/month=${String(today.getMonth()+1).padStart(2,'0')}/${path.basename(csvPath)}`;

    try {
      const uploadResult = await uploadToMinIO(csvPath, RAW_BUCKET, s3Key, 'text/csv');
      pass(`Uploaded raw CSV → s3://${RAW_BUCKET}/${s3Key} (${(uploadResult.size/1024).toFixed(1)} KB)`);
    } catch (e) {
      fail(`Raw CSV upload failed: ${e.message.slice(0, 120)}`);
    }
  }

  // ── Step 3: Transform (raw → silver) ─────────────────────────────────────
  step('Step 3: Transform Raw → Silver (business logic)');

  let silverRows = [];
  let filteredCount = 0;

  for (const raw of rawRows) {
    const silver = transformRow(raw);
    if (silver) {
      silverRows.push(silver);
    } else {
      filteredCount++;
    }
  }

  pass(`Transformed ${silverRows.length} rows (filtered ${filteredCount} bad rows)`);

  // Data quality assertions
  const allDistOk = silverRows.every(r => r.trip_distance > 0 && r.trip_distance <= 200);
  const allFareOk = silverRows.every(r => r.fare_amount > 0 && r.fare_amount <= 500);
  const allDurOk  = silverRows.every(r => r.trip_duration_minutes > 0 && r.trip_duration_minutes <= 300);

  allDistOk ? pass('DQ: all trip_distance values in range (0–200 mi)') : fail('DQ: trip_distance out of range!');
  allFareOk ? pass('DQ: all fare_amount values in range ($0–$500)')    : fail('DQ: fare_amount out of range!');
  allDurOk  ? pass('DQ: all trip_duration_minutes in range (0–300 min)') : fail('DQ: trip_duration out of range!');

  // Feature engineering check
  const hasTimeOfDay = silverRows.every(r => ['morning_rush','midday','evening_rush','evening','overnight'].includes(r.time_of_day));
  hasTimeOfDay ? pass('DQ: time_of_day feature correctly computed') : fail('DQ: time_of_day has invalid values!');

  // ── Step 4: Write Parquet (silver layer) ─────────────────────────────────
  step('Step 4: Write Silver Parquet File');

  const silverParquetPath = '/tmp/silver_trips.parquet';

  const silverSchema = {
    vendor_id:             { type: 'INT32' },
    pickup_datetime:       { type: 'UTF8' },
    dropoff_datetime:      { type: 'UTF8' },
    pickup_date:           { type: 'UTF8' },
    pickup_hour:           { type: 'INT32' },
    day_of_week:           { type: 'INT32' },
    day_name:              { type: 'UTF8' },
    is_weekend:            { type: 'BOOLEAN' },
    passenger_count:       { type: 'INT32' },
    trip_distance:         { type: 'DOUBLE' },
    pu_location_id:        { type: 'INT32' },
    do_location_id:        { type: 'INT32' },
    payment_type:          { type: 'INT32' },
    payment_type_label:    { type: 'UTF8' },
    fare_amount:           { type: 'DOUBLE' },
    tip_amount:            { type: 'DOUBLE' },
    total_amount:          { type: 'DOUBLE' },
    trip_duration_minutes: { type: 'DOUBLE' },
    avg_speed_mph:         { type: 'DOUBLE' },
    time_of_day:           { type: 'UTF8' },
    was_tipped:            { type: 'BOOLEAN' },
    tip_pct:               { type: 'DOUBLE' },
    fare_per_mile:         { type: 'DOUBLE' },
    revenue_per_minute:    { type: 'DOUBLE' },
  };

  try {
    const actualPath = await writeParquet(silverRows, silverParquetPath, silverSchema);
    const size = fs.statSync(actualPath).size;
    pass(`Silver file written: ${actualPath} (${(size/1024).toFixed(1)} KB)`);

    // Upload silver Parquet to MinIO
    if (minioUp) {
      try {
        const s3Key = `silver/trips/pickup_date=2024-01/part-00000.${actualPath.endsWith('.parquet') ? 'parquet' : 'jsonl'}`;
        const ct = actualPath.endsWith('.parquet') ? 'application/octet-stream' : 'application/x-ndjson';
        const r = await uploadToMinIO(actualPath, SILVER_BUCKET, s3Key, ct);
        pass(`Uploaded silver → s3://${SILVER_BUCKET}/${s3Key} (${(r.size/1024).toFixed(1)} KB)`);
      } catch (e) {
        fail(`Silver upload failed: ${e.message.slice(0,120)}`);
      }
    }
  } catch (e) {
    fail(`Parquet write failed: ${e.message}`);
  }

  // ── Step 5: Compute Gold aggregation ─────────────────────────────────────
  step('Step 5: Compute Gold Layer (daily summary)');

  const goldRows = computeGoldSummary(silverRows);

  const days    = new Set(goldRows.map(r => r.pickup_date)).size;
  const revenue = goldRows.reduce((s, r) => s + r.total_revenue, 0);
  const trips   = goldRows.reduce((s, r) => s + r.trip_count, 0);

  pass(`Gold aggregation: ${goldRows.length} hourly buckets across ${days} days`);
  pass(`Total trips: ${trips.toLocaleString()}`);
  pass(`Total revenue: $${revenue.toFixed(2)}`);
  pass(`Avg fare: $${(revenue/trips).toFixed(2)}`);

  // Write gold as JSONL (no Parquet needed for demo)
  const goldPath = '/tmp/gold_daily_summary.jsonl';
  fs.writeFileSync(goldPath, goldRows.map(r => JSON.stringify(r)).join('\n'), 'utf8');
  pass(`Gold file written: ${goldPath} (${(fs.statSync(goldPath).size/1024).toFixed(1)} KB)`);

  if (minioUp) {
    try {
      const goldParquetPath = '/tmp/gold_daily_summary.parquet';
      const goldSchema = {
        pickup_date:           { type: 'UTF8' },
        pickup_hour:           { type: 'INT32' },
        time_of_day:           { type: 'UTF8' },
        day_of_week:           { type: 'INT32' },
        day_name:              { type: 'UTF8' },
        is_weekend:            { type: 'BOOLEAN' },
        trip_count:            { type: 'INT32' },
        total_revenue:         { type: 'DOUBLE' },
        avg_fare:              { type: 'DOUBLE' },
        avg_distance:          { type: 'DOUBLE' },
        avg_duration_minutes:  { type: 'DOUBLE' },
        avg_tip:               { type: 'DOUBLE' },
        tip_rate_pct:          { type: 'DOUBLE' },
        total_passengers:      { type: 'INT32' },
        revenue_per_mile:      { type: 'DOUBLE' },
      };

      try {
        const actualGoldPath = await writeParquet(goldRows, goldParquetPath, goldSchema);
        const s3Key = `gold/daily_summary/pickup_date=2024-01/part-00000.${actualGoldPath.endsWith('.parquet') ? 'parquet' : 'jsonl'}`;
        const ct = actualGoldPath.endsWith('.parquet') ? 'application/octet-stream' : 'application/x-ndjson';
        const r = await uploadToMinIO(actualGoldPath, 'warehouse-gold', s3Key, ct);
        pass(`Uploaded gold → s3://warehouse-gold/${s3Key} (${(r.size/1024).toFixed(1)} KB)`);
      } catch (e) {
        // Try JSONL upload as fallback
        const s3Key = 'gold/daily_summary/pickup_date=2024-01/part-00000.jsonl';
        const r = await uploadToMinIO(goldPath, 'warehouse-gold', s3Key, 'application/x-ndjson');
        pass(`Uploaded gold (JSONL) → s3://warehouse-gold/${s3Key} (${(r.size/1024).toFixed(1)} KB)`);
      }
    } catch (e) {
      fail(`Gold upload failed: ${e.message.slice(0,120)}`);
    }
  }

  // ── Step 6: Sample output ─────────────────────────────────────────────────
  step('Step 6: Sample Output');

  console.log('\n  Top 5 hours by revenue:');
  console.log('  ' + '─'.repeat(70));
  console.log(`  ${'Date'.padEnd(12)} ${'Hour'.padEnd(6)} ${'Period'.padEnd(15)} ${'Trips'.padEnd(8)} ${'Revenue'.padEnd(10)} ${'Avg Fare'}`);
  console.log('  ' + '─'.repeat(70));

  [...goldRows]
    .sort((a, b) => b.total_revenue - a.total_revenue)
    .slice(0, 5)
    .forEach(r => {
      console.log(
        `  ${r.pickup_date.padEnd(12)} ` +
        `${String(r.pickup_hour).padEnd(6)} ` +
        `${r.time_of_day.padEnd(15)} ` +
        `${String(r.trip_count).padEnd(8)} ` +
        `$${r.total_revenue.toFixed(2).padEnd(9)} ` +
        `$${r.avg_fare.toFixed(2)}`
      );
    });
  console.log('  ' + '─'.repeat(70));

  // ── Results ───────────────────────────────────────────────────────────────
  console.log(`\n${C.bold}${C.cyan}═══════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}Pipeline Results${C.reset}`);
  console.log(`  ${C.green}Passed:  ${results.passed}${C.reset}`);
  console.log(`  ${C.red}Failed:  ${results.failed}${C.reset}`);
  console.log(`  ${C.yellow}Skipped: ${results.skipped}${C.reset}`);
  console.log(`${C.bold}${C.cyan}═══════════════════════════════════════${C.reset}\n`);

  if (results.failed === 0) {
    console.log(`${C.green}${C.bold}✅ Pipeline complete!${C.reset}`);
    console.log(`\n  Silver file: /tmp/silver_trips.parquet (or .jsonl)`);
    console.log(`  Gold file:   /tmp/gold_daily_summary.jsonl`);
    if (minioUp) {
      console.log(`  MinIO raw:   s3://${RAW_BUCKET}/nyc_taxi/...`);
      console.log(`  MinIO silver: s3://${SILVER_BUCKET}/silver/trips/...`);
      console.log(`  MinIO gold:   s3://warehouse-gold/gold/daily_summary/...`);
    }
  } else {
    console.log(`${C.red}${C.bold}❌ ${results.failed} step(s) failed${C.reset}`);
    process.exit(1);
  }
}

main().catch(e => {
  err(`Unhandled error: ${e.message}`);
  console.error(e.stack);
  process.exit(1);
});
