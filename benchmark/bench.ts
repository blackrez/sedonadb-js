import { SessionContext, ipcToRows } from '../index.js'
import { existsSync } from 'fs'
import { join } from 'path'

// ── Configuration ──────────────────────────────────────────────────────
const DATA_DIR = process.env.DATA_DIR || ''
const TABLES = ['building', 'customer', 'driver', 'trip', 'vehicle', 'zone']
const hasData = DATA_DIR !== '' && TABLES.some((t) => existsSync(join(DATA_DIR, t)))
const QUERY_TIMEOUT_MS = 60_000 // 60s per query

// ── SpatialBench SQL Queries (SedonaDB dialect) ───────────────────────
const queries: Record<string, string> = {
  q1: `-- Q1: Trips starting within 50 km of Sedona city center, ordered by distance
SELECT
   t.t_tripkey,
   ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon,
   ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat,
   t.t_pickuptime,
   ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('POINT (-111.7610 34.8697)')) AS distance_to_center
FROM trip t
WHERE ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('POINT (-111.7610 34.8697)'), 0.45)
ORDER BY distance_to_center ASC, t.t_tripkey ASC`,

  q2: `-- Q2: Count trips starting within Coconino County (Arizona) zone
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(
   ST_GeomFromWKB(t.t_pickuploc),
   (SELECT ST_GeomFromWKB(z.z_boundary) FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1)
)`,

  q3: `-- Q3: Monthly trip statistics within 15 km radius of Sedona city center
SELECT
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
   COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_distance) AS avg_distance,
   AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
   AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_DWithin(
         ST_GeomFromWKB(t.t_pickuploc),
         ST_GeomFromText('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))'),
         0.045
     )
GROUP BY pickup_month
ORDER BY pickup_month`,

  q4: `-- Q4: Zone distribution of top 1000 trips by tip amount
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM zone z
   JOIN (
      SELECT t.t_pickuploc
      FROM trip t
      ORDER BY t.t_tip DESC, t.t_tripkey ASC
      LIMIT 1000
   ) top_trips ON ST_Within(ST_GeomFromWKB(top_trips.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC`,

  q5: `-- Q5 (SedonaDB): Monthly travel patterns for repeat customers (convex hull via ST_Collect_Agg)
SELECT
   c.c_custkey,
   c.c_name AS customer_name,
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
   ST_Area(ST_ConvexHull(ST_Collect_Agg(ST_GeomFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area,
   COUNT(*) as dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5
ORDER BY dropoff_count DESC, c.c_custkey ASC`,

  q6: `-- Q6: Zone statistics for trips intersecting a bounding box
SELECT
   z.z_zonekey,
   z.z_name,
   COUNT(t.t_tripkey) AS total_pickups,
   AVG(t.t_totalamount) AS avg_distance,
   AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(
         ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'),
         ST_GeomFromWKB(z.z_boundary)
     )
  AND ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC`,

  q7: `-- Q7: Detect potential route detours by comparing reported vs geometric distances
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       t.t_distance AS reported_distance_m,
       ST_Length(ST_MakeLine(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(t.t_dropoffloc))) / 0.000009 AS line_distance_m
   FROM trip t
)
SELECT
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC`,

  q8: `-- Q8: Count nearby pickups for each building within 500 m radius
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t JOIN building b ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045)
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC`,

  q9: `-- Q9: Building conflation (duplicate / overlap detection via IoU)
WITH b1 AS (
   SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom FROM building
),
b2 AS (
   SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom FROM building
),
pairs AS (
   SELECT
       b1.id AS building_1,
       b2.id AS building_2,
       ST_Area(b1.geom) AS area1,
       ST_Area(b2.geom) AS area2,
       ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
   FROM b1 JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom)
)
SELECT
   building_1,
   building_2,
   area1,
   area2,
   overlap_area,
   CASE
       WHEN overlap_area = 0 THEN 0.0
       WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
       ELSE overlap_area / (area1 + area2 - overlap_area)
   END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC`,

  q10: `-- Q10: Zone statistics for trips starting within each zone (LEFT JOIN preserves all zones)
SELECT
   z.z_zonekey,
   z.z_name AS pickup_zone,
   AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
   AVG(t.t_distance) AS avg_distance,
   COUNT(t.t_tripkey) AS num_trips
FROM zone z LEFT JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC`,

  q11: `-- Q11: Count trips that cross between different zones
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
   JOIN zone pickup_zone ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
   JOIN zone dropoff_zone ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey`,

  q12: `-- Q12: Find 5 nearest buildings to each trip pickup location (KNN join)
WITH trip_with_geom AS (
   SELECT t_tripkey, t_pickuploc, ST_GeomFromWKB(t_pickuploc) as pickup_geom FROM trip
),
building_with_geom AS (
   SELECT b_buildingkey, b_name, b_boundary, ST_GeomFromWKB(b_boundary) as boundary_geom FROM building
)
SELECT
   t.t_tripkey,
   t.t_pickuploc,
   b.b_buildingkey,
   b.b_name AS building_name,
   ST_Distance(t.pickup_geom, b.boundary_geom) AS distance_to_building
FROM trip_with_geom t JOIN building_with_geom b ON ST_KNN(t.pickup_geom, b.boundary_geom, 5, FALSE)
ORDER BY distance_to_building ASC, b.b_buildingkey ASC`,
}

// ── Helpers ────────────────────────────────────────────────────────────

function parquetPath(table: string): string {
  const dirPath = join(DATA_DIR, table)
  if (existsSync(dirPath)) return dirPath
  const filePath = join(DATA_DIR, `${table}.parquet`)
  if (existsSync(filePath)) return filePath
  return dirPath
}

function pad(s: string, n: number): string {
  return s + ' '.repeat(Math.max(0, n - s.length))
}

async function timedRun<T>(fn: () => Promise<T>, timeoutMs: number): Promise<{ ok: true; timeMs: number } | { ok: false; timeMs: number; error: string }> {
  const start = performance.now()
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const result = await Promise.race([
      fn(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs)
      }),
    ])
    clearTimeout(timer)
    return { ok: true, timeMs: Math.round(performance.now() - start) }
  } catch (e: any) {
    clearTimeout(timer)
    const msg = e?.message ?? String(e)
    // Truncate very long messages
    return { ok: false, timeMs: Math.round(performance.now() - start), error: msg.slice(0, 120) }
  }
}

// ── Main ───────────────────────────────────────────────────────────────

const ctx = await SessionContext.newLocalInteractive()
const results: { name: string; timeMs: number | string; rows?: number }[] = []

// ── Data loading ──────────────────────────────────────────────────
if (!hasData) {
  console.log(`\n  ⚠  DATA_DIR not set or no parquet files found at "${DATA_DIR}"`)
  console.log('  ⚠  Set DATA_DIR to a SpatialBench parquet directory.\n')
} else {
  for (const table of TABLES) {
    const path = parquetPath(table)
    process.stdout.write(`  Loading ${pad(table, 10)} ← ${path}`)
    const r = await timedRun(() => ctx.registerParquetTable(table, path, true), 120_000)
    if (r.ok) {
      console.log(`  ✓`)
    } else {
      console.log(`  ✗ ${r.error}`)
    }
  }
  console.log('')
}

// ── Benchmark runner ──────────────────────────────────────────────

interface BenchItem {
  name: string
  run: () => Promise<any>
  skip?: boolean
}

const items: BenchItem[] = []

// 1. SpatialBench queries — use streamSql because sql() can crash
//    when the result contains geometry binary data (ST_Collect_Agg etc.)
if (hasData) {
  for (let i = 1; i <= 12; i++) {
    const key = `q${i}`
    const sql = queries[key]
    items.push({
      name: `SpatialBench ${key}`,
      run: async () => {
        const stream = ctx.streamSql(sql)
        for await (const _row of stream) { /* consume */ }
      },
    })
  }
}

// 2. Spatial-adapted extras
if (hasData) {
  items.push({
    name: 'SQL + ipcToRows: WKB decode (trip 10K)',
    run: async () => {
      const r = await ctx.sql('SELECT t_tripkey, t_pickuploc, t_dropoffloc FROM trip LIMIT 10000')
      ipcToRows(r.arrowIpc)
    },
  })
  items.push({
    name: 'StreamSql: trip 10K pickup locations',
    run: async () => {
      const stream = ctx.streamSql('SELECT t_tripkey, t_pickuploc FROM trip LIMIT 10000')
      for await (const _row of stream) { /* consume */ }
    },
  })
  items.push({
    name: 'SQL collect: spatial filter (Q2)',
    run: () => ctx.sql(queries.q2),
  })
  items.push({
    name: 'StreamSql: spatial filter (Q2)',
    run: async () => {
      const stream = ctx.streamSql(queries.q2)
      for await (const _row of stream) { /* consume */ }
    },
  })
}

// 3. Basic functional sanity (always)
items.push({
  name: 'SQL: ST_Point + ST_AsText',
  run: () => ctx.sqlRows("SELECT ST_AsText(ST_Point(30, 10)) AS geom"),
})
items.push({
  name: 'SQL: ST_Area on polygon',
  run: () => ctx.sqlRows("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS area"),
})
items.push({
  name: 'SQL + ipcToRows: 10K rows',
  run: async () => {
    const r = await ctx.sql('SELECT generate_series(1, 10000) AS n')
    ipcToRows(r.arrowIpc)
  },
})
items.push({
  name: 'SQL + ipcToRows: spatial 10K points',
  run: async () => {
    const r = await ctx.sql("SELECT ST_Point(CAST(n AS DOUBLE), CAST(n AS DOUBLE)) AS geom FROM generate_series(1, 10000) AS t(n)")
    ipcToRows(r.arrowIpc)
  },
})
items.push({
  name: 'StreamSql: spatial 10K points',
  run: async () => {
    const stream = ctx.streamSql("SELECT ST_Point(CAST(n AS DOUBLE), CAST(n AS DOUBLE)) AS geom FROM generate_series(1, 10000) AS t(n)")
    for await (const _row of stream) { /* consume */ }
  },
})

// ── Run all benchmarks ───────────────────────────────────────────
const totalStart = performance.now()
for (const item of items) {
  process.stdout.write(`  ${pad(item.name, 48)} `)
  const r = await timedRun(item.run, QUERY_TIMEOUT_MS)
  if (r.ok) {
    results.push({ name: item.name, timeMs: r.timeMs })
    console.log(`${String(r.timeMs).padStart(6)} ms`)
  } else {
    results.push({ name: item.name, timeMs: r.error })
    console.log(` ${'TIMEOUT'.padStart(8)}  ${r.error}`)
  }
}
const totalTime = Math.round(performance.now() - totalStart)
console.log('')

// ── Results table ─────────────────────────────────────────────────
console.log('─'.repeat(70))
console.log(`  ${pad('Benchmark', 48)} ${pad('Time', 10)}`)
console.log('─'.repeat(70))
for (const r of results) {
  const timeStr = typeof r.timeMs === 'number' ? `${r.timeMs} ms` : r.timeMs
  console.log(`  ${pad(r.name, 48)} ${pad(timeStr, 10)}`)
}
console.log('─'.repeat(70))
console.log(`  ${pad('Total', 48)} ${pad(`${totalTime} ms`, 10)}`)
console.log('')
