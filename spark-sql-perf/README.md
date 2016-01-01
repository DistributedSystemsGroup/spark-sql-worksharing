# Spark SQL Performance Tests

## Configuration
- sparkVersion := "1.5.2" (in build.sbt)
- remove the account credential and stuffs declared in build.sbt: dbcUsername, dbcPassword, dbcApiUrl, dbcClusters, dbcLibraryPath

# Different sparkVersion build
in Benchmark.scala
- for sparkVersion = 1.6+, check the [commit](https://github.com/databricks/spark-sql-perf/commit/344b31ed69f18205fb8192df2f5a8704e6a62615) 
 `case UnresolvedRelation(t, _) => t.table`
 `tableIdentifier.table`
- for sparkVersion = 1.4, 1.5
 `case UnresolvedRelation(Seq(name), _) => name`
 `tableIdentifier.last`

## Build
`./build/sbt package`

## How to use?
- Refer to project spark-sql-tpc-ds-perf for an example of usage.