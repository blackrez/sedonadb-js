import test from 'ava'
import { ContextBuilder, SessionContext, ipcToRows } from '../index'

// ---------------------------------------------------------------------------
// Session & basic query tests
// ---------------------------------------------------------------------------

test('create session context', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  t.truthy(ctx)
})

test('run basic SQL query', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql("SELECT ST_AsText(ST_Point(30, 10)) AS geom")

  t.is(result.columns.length, 1)
  t.is(result.columns[0], 'geom')
  t.is(result.numRows, 1)
  t.is(result.rows[0][0], 'POINT(30 10)') // string column
})

test('run SQL with multiple rows', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    'SELECT * FROM (VALUES (1, \'one\'), (2, \'two\'), (3, \'three\')) AS t (id, name)',
  )

  t.is(result.columns.length, 2)
  t.is(result.columns[0], 'id')
  t.is(result.columns[1], 'name')
  t.is(result.numRows, 3)
  t.is(result.rows[0][1], 'one')   // string column
  t.is(result.rows[2][1], 'three') // string column
  t.is(result.rows[0][0], 1)       // integer column → JS number
  t.is(result.rows[1][0], 2)
  t.is(result.rows[2][0], 3)
})

test('DataFrame schema via columns', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql("SELECT ST_Point(30, 10) AS geom, 1 AS num")

  t.is(result.columns.length, 2)
  t.is(result.columns[0], 'geom')
  t.is(result.columns[1], 'num')
})

test('DataFrame showSedona output', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql('SELECT 1 AS one, 2 AS two')
  t.is(result.numRows, 1)
  t.is(result.columns.join(', '), 'one, two')
})

test('DataFrame collect returns correct rows', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    'SELECT * FROM (VALUES (\'a\', 1), (\'b\', 2)) AS t (letter, number) ORDER BY letter',
  )

  t.is(result.numRows, 2)
  t.is(result.rows[0][0], 'a') // string column
  t.is(result.rows[0][1], 1)   // integer column → JS number
  t.is(result.rows[1][0], 'b')
  t.is(result.rows[1][1], 2)
})

test('DataFrame limit', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    'SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t (x)',
  )
  t.is(result.numRows, 5)
})

test('spatial function: ST_Area', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS area",
  )

  t.is(result.numRows, 1)
  // Area is a float column → JS number
  t.is(result.rows[0][0], 1.0)
})

test('spatial function: ST_Buffer', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 1.0)) AS buff",
  )

  t.is(result.numRows, 1)
  const wkt = result.rows[0][0] as string // string column
  t.truthy(wkt)
  t.true(wkt.startsWith('POLYGON') || wkt.startsWith('MULTIPOLYGON'))
})

test('handle null values', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    'SELECT * FROM (VALUES (1, \'a\'), (2, NULL)) AS t (id, val) ORDER BY id',
  )

  t.is(result.numRows, 2)
  t.is(result.rows[0][1], 'a') // non-null string
  t.is(result.rows[1][1], null) // null → JS null!
})

test('query with aggregation', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    'SELECT count(*) AS cnt FROM (VALUES (1), (2), (3), (4), (5)) AS t (x)',
  )

  t.is(result.numRows, 1)
  t.is(result.rows[0][0], 5) // count → Int64 → JS number
})

test('read remote GeoParquet and query with WKT geometry', async (t) => {
  const url =
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet'

  const ctx = await SessionContext.newLocalInteractive()

  const df = await ctx.readParquet(url)
  ctx.registerTable('cities', df)

  const result = await ctx.sql(
    'SELECT name, ST_AsText(geometry) AS geom FROM cities ORDER BY name LIMIT 5',
  )

  t.is(result.numRows, 5)
  t.is(result.columns.length, 2)
  t.is(result.columns[0], 'name')
  t.is(result.columns[1], 'geom')

  t.is(result.rows[0][0], 'Abidjan')
  t.true((result.rows[0][1] as string).startsWith('POINT('))
  t.is(result.rows[4][0], 'Addis Ababa')
  t.true((result.rows[4][1] as string).startsWith('POINT('))
})

// ---------------------------------------------------------------------------
// ContextBuilder tests
// ---------------------------------------------------------------------------

test('ContextBuilder default build (interactive)', async (t) => {
  const ctx = await new ContextBuilder().build()
  const result = await ctx.sql("SELECT ST_AsText(ST_Point(1, 2)) AS geom")
  t.is(result.rows[0][0], 'POINT(1 2)')
})

test('ContextBuilder with memory limit string', async (t) => {
  const ctx = await new ContextBuilder().memoryLimit('512m').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder without memory limit', async (t) => {
  const ctx = await new ContextBuilder().withoutMemoryLimit().build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder with fair pool type', async (t) => {
  const ctx = await new ContextBuilder().poolType('fair').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder with greedy pool type', async (t) => {
  const ctx = await new ContextBuilder().poolType('greedy').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder with unlimited memory', async (t) => {
  const ctx = await new ContextBuilder().memoryLimit('unlimited').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder non-interactive mode', async (t) => {
  const ctx = await new ContextBuilder().localInteractive(false).build()
  const result = await ctx.sql("SELECT ST_AsText(ST_Point(3, 4)) AS geom")
  t.is(result.rows[0][0], 'POINT(3 4)')
})

test('ContextBuilder: non-interactive session with plain SQL', async (t) => {
  const ctx = await new ContextBuilder().localInteractive(false).build()
  const result = await ctx.sql('SELECT 42 AS answer')
  t.is(result.rows[0][0], 42)
  t.is(result.numRows, 1)
  t.is(result.columns[0], 'answer')
})

test('ContextBuilder: unspillable_reserve_ratio', async (t) => {
  const ctx = await new ContextBuilder()
    .unspillableReserveRatio(0.3)
    .build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder: SessionContext.new() delegates to builder', async (t) => {
  const ctx = await SessionContext.new()
  const result = await ctx.sql("SELECT ST_AsText(ST_Point(5, 6)) AS geom")
  t.is(result.rows[0][0], 'POINT(5 6)')
})

test('ContextBuilder: chaining multiple options', async (t) => {
  const ctx = await new ContextBuilder()
    .memoryLimit('1g')
    .poolType('greedy')
    .build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder: tempDir option', async (t) => {
  const ctx = await new ContextBuilder()
    .tempDir('/tmp/sedona-test')
    .build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
})

test('ContextBuilder: error on invalid memory limit string', async (t) => {
  const err = await t.throwsAsync(async () => {
    await new ContextBuilder().memoryLimit('xyz').build()
  })
  t.truthy(err)
})

test('ContextBuilder: error on invalid pool type', async (t) => {
  const err = await t.throwsAsync(async () => {
    await new ContextBuilder().poolType('weird').build()
  })
  t.truthy(err)
})

test('ContextBuilder: error on unspillableReserveRatio negative', async (t) => {
  const err = await t.throwsAsync(async () => {
    await new ContextBuilder().unspillableReserveRatio(-0.1).build()
  })
  t.truthy(err)
})

test('ContextBuilder: error on unspillableReserveRatio > 1.0', async (t) => {
  const err = await t.throwsAsync(async () => {
    await new ContextBuilder().unspillableReserveRatio(1.5).build()
  })
  t.truthy(err)
})

test('ContextBuilder: builder methods return independent instances', async (t) => {
  const base = new ContextBuilder()
  const withLimit = base.memoryLimit('2g')
  const ctx = await base.build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], 1)
  const ctx2 = await withLimit.build()
  const result2 = await ctx2.sql('SELECT 1 AS x')
  t.is(result2.rows[0][0], 1)
})

test('DataFrame toView: register a view and query it', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )

  df.toView(ctx, 'my_cities')

  const result = await ctx.sql(
    "SELECT name, ST_AsText(geometry) AS geom FROM my_cities ORDER BY name LIMIT 3",
  )

  t.is(result.numRows, 3)
  t.is(result.columns[0], 'name')
  t.is(result.columns[1], 'geom')
  t.is(result.rows[0][0], 'Abidjan')
  t.true((result.rows[0][1] as string).startsWith('POINT('))
})

test('DataFrame toView: overwrite an existing view', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )

  df.toView(ctx, 'overwrite_cities')

  const result1 = await ctx.sql('SELECT count(*) AS cnt FROM overwrite_cities')
  const count1 = result1.rows[0][0] as number // count → number
  t.true(count1 > 0)

  df.toView(ctx, 'overwrite_cities', true)

  const result2 = await ctx.sql('SELECT count(*) AS cnt FROM overwrite_cities')
  const count2 = result2.rows[0][0] as number
  t.is(count1, count2)
})

test('DataFrame toView: error on duplicate without overwrite', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )

  df.toView(ctx, 'duplicate_view')

  t.throws(() => {
    df.toView(ctx, 'duplicate_view')
  }, { message: /already exists/i })
})

// ---------------------------------------------------------------------------
// Arrow IPC format & typed-rows tests
// ---------------------------------------------------------------------------

test('QueryResult has typed rows and arrowIpc buffer', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql('SELECT 1 AS x')

  t.true(Array.isArray(result.rows))
  t.is(result.rows[0][0], 1) // number, not string
  t.true(Array.isArray(result.arrowIpc))
  t.true(result.arrowIpc.length > 0)
})

test('rows have correct JS types for mixed columns', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT 1 AS num, 'hello' AS str, true AS flag, NULL AS nothing",
  )

  t.is(result.numRows, 1)
  t.is(typeof result.rows[0][0], 'number')
  t.is(result.rows[0][0], 1)
  t.is(typeof result.rows[0][1], 'string')
  t.is(result.rows[0][1], 'hello')
  t.is(typeof result.rows[0][2], 'boolean')
  t.is(result.rows[0][2], true)
  t.is(result.rows[0][3], null)
})

test('ipcToRows still works for raw IPC buffers', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t (id, name) ORDER BY id",
  )

  // ipcToRows returns strings (for backward compat)
  const stringRows = ipcToRows(result.arrowIpc)
  t.is(stringRows.length, 2)
  t.is(stringRows[0][0], '1')   // stringified
  t.is(stringRows[0][1], 'a')
  t.is(stringRows[1][0], '2')   // stringified

  // Direct rows have proper types
  t.is(result.rows[0][0], 1)     // number
  t.is(result.rows[0][1], 'a')
  t.is(result.rows[1][0], 2)     // number
})

test('ipcToRows handles null values', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT * FROM (VALUES (1, 'hello'), (2, NULL)) AS t (id, val) ORDER BY id",
  )

  // ipcToRows returns empty string for null (backward compat)
  const stringRows = ipcToRows(result.arrowIpc)
  t.is(stringRows[1][1], '')

  // Direct rows have proper null
  t.is(result.rows[1][1], null)
})

test('ipcToRows handles empty result', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT * FROM (VALUES (1)) AS t (x) WHERE x > 100",
  )

  t.is(result.numRows, 0)
  t.is(result.rows.length, 0)
  t.true(Array.isArray(result.arrowIpc))

  const rows = ipcToRows(result.arrowIpc)
  t.is(rows.length, 0)
})

test('DataFrame.collect() returns QueryResult with typed rows', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )

  const limited = await df.select(['name']).then(d => d.limit(3))
  const result = await limited.collect()

  t.truthy(result.columns)
  t.true(result.numRows > 0)
  t.is(result.rows.length, 3)
  t.true(Array.isArray(result.arrowIpc))
  t.true(result.arrowIpc.length > 0)
  t.is(typeof result.rows[0][0], 'string')
  t.truthy(result.rows[0][0])
})

// ---------------------------------------------------------------------------
// Stream API tests
// ---------------------------------------------------------------------------

/** Consume a ReadableStream and return all rows as an array. */
async function collectStream(stream: ReadableStream<Array<string>>): Promise<Array<Array<string>>> {
  const reader = stream.getReader()
  const rows: Array<Array<string>> = []
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    rows.push(value)
  }
  return rows
}

test('streamSql returns a ReadableStream', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql('SELECT 1 AS x')
  t.truthy(stream)
  t.truthy(typeof stream.getReader === 'function')
})

test('streamSql data matches sql()', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql('SELECT 1 AS x, 2 AS y')

  const stream = ctx.streamSql('SELECT 1 AS x, 2 AS y')
  const streamRows = await collectStream(stream)

  t.is(streamRows.length, result.numRows)
  // stream returns strings, rows are numbers → compare stringified
  t.is(streamRows[0][0], String(result.rows[0][0]))
  t.is(streamRows[0][1], String(result.rows[0][1]))
})

test('streamSql with multiple rows', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql(
    "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (id, name) ORDER BY id",
  )
  const rows = await collectStream(stream)

  t.is(rows.length, 3)
  t.is(rows[0][0], '1')
  t.is(rows[0][1], 'one')
  t.is(rows[1][0], '2')
  t.is(rows[1][1], 'two')
  t.is(rows[2][0], '3')
  t.is(rows[2][1], 'three')
})

test('streamSql handles null values', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql(
    "SELECT * FROM (VALUES (1, 'a'), (2, NULL)) AS t (id, val) ORDER BY id",
  )
  const rows = await collectStream(stream)

  t.is(rows.length, 2)
  t.is(rows[0][1], 'a')
  t.is(rows[1][1], '') // stream returns empty string for null
})

test('streamSql with aggregation', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql(
    'SELECT count(*) AS cnt FROM (VALUES (1), (2), (3), (4), (5)) AS t (x)',
  )
  const rows = await collectStream(stream)

  t.is(rows.length, 1)
  t.is(rows[0][0], '5')
})

test('DataFrame.stream() after readParquet', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )
  const stream = df.stream()
  t.truthy(stream)
  t.truthy(typeof stream.getReader === 'function')
})

test('DataFrame.stream() data matches collect() using limit', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )

  const limited = await df.limit(5)
  const result = await limited.collect()
  const streamRows = await collectStream(limited.stream())

  t.is(streamRows.length, result.numRows)
  t.is(streamRows.length, 5)
  t.truthy(streamRows[0][0])
  // Compare stringified versions (stream returns strings, rows are typed)
  for (let i = 0; i < streamRows.length; i++) {
    for (let j = 0; j < streamRows[i].length; j++) {
      t.is(streamRows[i][j], String(result.rows[i][j]))
    }
  }
})

test('DataFrame.stream() with spatial columns', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )

  const projected = await df.select(['name', 'geometry'])
  const limited = await projected.limit(3)
  const streamRows = await collectStream(limited.stream())

  t.is(streamRows.length, 3)
  for (const row of streamRows) {
    t.is(row.length, 2)
    t.truthy(row[0])
    t.truthy(row[1])
  }
})

test('streamSql with spatial ST_AsText', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql("SELECT ST_AsText(ST_Point(30, 10)) AS geom")
  const rows = await collectStream(stream)

  t.is(rows.length, 1)
  t.is(rows[0][0], 'POINT(30 10)')
})

test('DataFrame.stream() cancel early (read partial)', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.readParquet(
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet',
  )

  const stream = df.stream()
  const reader = stream.getReader()
  const first = await reader.read()
  t.false(first.done)
  t.truthy(first.value)
  t.true(first.value.length > 0)

  const second = await reader.read()
  t.false(second.done)
  t.truthy(second.value)

  await reader.cancel()
  t.pass('stream canceled without error')
})

test('streamSql cancel early (read none)', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql(
    'SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t (x)',
  )
  const reader = stream.getReader()
  await reader.cancel()
  t.pass('streamSql canceled before any reads')
})

// ---------------------------------------------------------------------------
// Parameterized query tests
// ---------------------------------------------------------------------------

test('sql with positional params ($1, $2)', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT * FROM (VALUES ($1, $2)) AS t (a, b)",
    [42, 'hello'],
  )
  t.is(result.numRows, 1)
  t.is(result.columns[0], 'a')
  t.is(result.columns[1], 'b')
  t.is(result.rows[0][0], 42.0)    // JS number param → Float64 → number
  t.is(result.rows[0][1], 'hello') // string param → string
})

test('sql with positional params filters correctly', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t (id, name) WHERE id > $1 ORDER BY id",
    [1],
  )
  t.is(result.numRows, 2)
  t.is(result.rows[0][1], 'b')
  t.is(result.rows[1][1], 'c')
})

test('sql with named params ($name)', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT * FROM (VALUES ($min, $max)) AS t (lo, hi)",
    { min: 10, max: 20 },
  )
  t.is(result.numRows, 1)
  t.is(result.rows[0][0], 10.0)
  t.is(result.rows[0][1], 20.0)
})

test('sql with boolean and null params', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT $1 AS bool_val, $2 AS null_val",
    [true, null],
  )
  t.is(result.numRows, 1)
  t.is(result.rows[0][0], true)  // boolean → JS boolean
  t.is(result.rows[0][1], null)  // null → JS null
})

test('sql without params still works', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql('SELECT 42 AS answer')
  t.is(result.rows[0][0], 42)
})

test('streamSql with positional params', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql(
    "SELECT * FROM (VALUES ($1, $2)) AS t (a, b)",
    [99, 'test'],
  )
  const rows = await collectStream(stream)
  t.is(rows.length, 1)
  t.is(rows[0][0], '99.0')
  t.is(rows[0][1], 'test')
})

test('streamSql with named params', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const stream = ctx.streamSql(
    "SELECT $lo AS low, $hi AS high",
    { lo: 1, hi: 100 },
  )
  const rows = await collectStream(stream)
  t.is(rows.length, 1)
  t.is(rows[0][0], '1.0')
  t.is(rows[0][1], '100.0')
})

test('sql with params and spatial function', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT ST_AsText(ST_Point($1, $2)) AS geom",
    [30, 10],
  )
  t.is(result.numRows, 1)
  t.is(result.rows[0][0], 'POINT(30 10)')
})
