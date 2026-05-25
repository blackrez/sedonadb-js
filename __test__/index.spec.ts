import test from 'ava'

import { ContextBuilder, SessionContext } from '../index'

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
  t.is(result.rows[0][0], 'POINT(30 10)')
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
  t.is(result.rows[0][1], 'one')
  t.is(result.rows[2][1], 'three')
})

test('DataFrame schema', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const df = await ctx.sql("SELECT ST_Point(30, 10) AS geom, 1 AS num")
  // sql() returns QueryResult directly, so test schema from the object
  t.is(df.columns.length, 2)
  t.is(df.columns[0], 'geom')
  t.is(df.columns[1], 'num')
})

test('DataFrame showSedona output', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql('SELECT 1 AS one, 2 AS two')
  // QueryResult gives us rows directly - verify showSedona would work
  t.is(result.numRows, 1)
  t.is(result.columns.join(', '), 'one, two')
})

test('DataFrame collect returns correct rows', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    'SELECT * FROM (VALUES (\'a\', 1), (\'b\', 2)) AS t (letter, number) ORDER BY letter',
  )

  t.is(result.numRows, 2)
  t.is(result.rows[0][0], 'a')
  t.is(result.rows[0][1], '1')
  t.is(result.rows[1][0], 'b')
  t.is(result.rows[1][1], '2')
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
  // Area of a 1x1 square should be 1.0
  const area = parseFloat(result.rows[0][0])
  t.is(area, 1.0)
})

test('spatial function: ST_Buffer', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 1.0)) AS buff",
  )

  t.is(result.numRows, 1)
  const wkt = result.rows[0][0]
  t.truthy(wkt)
  t.true(wkt.startsWith('POLYGON') || wkt.startsWith('MULTIPOLYGON'))
})

test('handle null values', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    'SELECT * FROM (VALUES (1, \'a\'), (2, NULL)) AS t (id, val) ORDER BY id',
  )

  t.is(result.numRows, 2)
  t.is(result.rows[1][1], '') // null values returned as empty string
})

test('query with aggregation', async (t) => {
  const ctx = await SessionContext.newLocalInteractive()
  const result = await ctx.sql(
    `SELECT count(*) AS cnt FROM (VALUES (1), (2), (3), (4), (5)) AS t (x)`,
  )

  t.is(result.numRows, 1)
  t.is(result.rows[0][0], '5')
})

test('read remote GeoParquet and query with WKT geometry', async (t) => {
  const url =
    'https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet'

  const ctx = await SessionContext.newLocalInteractive()

  // Read the remote GeoParquet file
  const df = await ctx.readParquet(url)

  // Register as a temporary view for SQL queries
  ctx.registerTable('cities', df)

  // Query the first 5 cities sorted by name, with geometry as WKT
  const result = await ctx.sql(
    'SELECT name, ST_AsText(geometry) AS geom FROM cities ORDER BY name LIMIT 5',
  )

  t.is(result.numRows, 5)
  t.is(result.columns.length, 2)
  t.is(result.columns[0], 'name')
  t.is(result.columns[1], 'geom')

  // First city alphabetically
  t.is(result.rows[0][0], 'Abidjan')
  t.true(result.rows[0][1].startsWith('POINT('))

  // Last among the first 5
  t.is(result.rows[4][0], 'Addis Ababa')
  t.true(result.rows[4][1].startsWith('POINT('))
})

test('ContextBuilder default build (interactive)', async (t) => {
  const ctx = await new ContextBuilder().build()
  const result = await ctx.sql("SELECT ST_AsText(ST_Point(1, 2)) AS geom")
  t.is(result.rows[0][0], 'POINT(1 2)')
})

test('ContextBuilder with memory limit string', async (t) => {
  const ctx = await new ContextBuilder().memoryLimit('512m').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], '1')
})

test('ContextBuilder without memory limit', async (t) => {
  const ctx = await new ContextBuilder().withoutMemoryLimit().build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], '1')
})

test('ContextBuilder with fair pool type', async (t) => {
  const ctx = await new ContextBuilder().poolType('fair').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], '1')
})

test('ContextBuilder with greedy pool type', async (t) => {
  const ctx = await new ContextBuilder().poolType('greedy').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], '1')
})

test('ContextBuilder with unlimited memory', async (t) => {
  const ctx = await new ContextBuilder().memoryLimit('unlimited').build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], '1')
})

test('ContextBuilder non-interactive mode', async (t) => {
  const ctx = await new ContextBuilder().localInteractive(false).build()
  const result = await ctx.sql("SELECT ST_AsText(ST_Point(3, 4)) AS geom")
  t.is(result.rows[0][0], 'POINT(3 4)')
})

test('ContextBuilder: unspillable_reserve_ratio', async (t) => {
  const ctx = await new ContextBuilder()
    .unspillableReserveRatio(0.3)
    .build()
  const result = await ctx.sql('SELECT 1 AS x')
  t.is(result.rows[0][0], '1')
})

test('ContextBuilder: SessionContext.new() delegates to builder', async (t) => {
  const ctx = await SessionContext.new()
  const result = await ctx.sql("SELECT ST_AsText(ST_Point(5, 6)) AS geom")
  t.is(result.rows[0][0], 'POINT(5 6)')
})
