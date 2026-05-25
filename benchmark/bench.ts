import { Bench } from 'tinybench'

import { SessionContext } from '../index.js'

const ctx = await SessionContext.newLocalInteractive()

const b = new Bench()

b.add('SQL: ST_Point + ST_AsText', async () => {
  await ctx.sql("SELECT ST_AsText(ST_Point(30, 10)) AS geom")
})

b.add('SQL: aggregation count', async () => {
  await ctx.sql("SELECT count(*) AS cnt FROM (VALUES (1), (2), (3), (4), (5)) AS t (x)")
})

b.add('SQL: ST_Area on polygon', async () => {
  await ctx.sql("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS area")
})

await b.run()

console.table(b.table())
