# Spark SQL Performance
This is the library provided by Databricks that supports running TPC-DS queries on Spark SQL.  
Repository URL: <https://github.com/databricks/spark-sql-perf>

You can get it by:
```
git clone https://github.com/databricks/spark-sql-perf.git
cd spark-sql-perf
// checkout the version 1.6
git checkout e516e1e7b31713efbc25b9c5caf6cf556ec064d9
// then, you need to edit the `build.sbt` file:
// change: sparkVersion := "1.6.1"
// remove the account credential requirements: dbcUsername, dbcPassword, dbcApiUrl, dbcClusters, dbcLibraryPath
```

### Modification required to support CSV format

```
//1. In Tables.scala, function genData
writer.format(format).option("header", "true").mode(mode)

//2. In Tables.scala, function createTemporaryTable
qlContext.read.format(format).option("header", "true").option("inferSchema", "true").load(location).registerTempTable(name)

```

### Different sparkVersion build
in Benchmark.scala
- for sparkVersion = 1.6+, check the [commit](https://github.com/databricks/spark-sql-perf/commit/344b31ed69f18205fb8192df2f5a8704e6a62615) 
 `case UnresolvedRelation(t, _) => t.table`
 `tableIdentifier.table`
- for sparkVersion = 1.4, 1.5
 `case UnresolvedRelation(Seq(name), _) => name`
 `tableIdentifier.last`

### How to build?
`./build/sbt package`

### How to use?
- Refer to project spark-sql-tpc-ds-perf for an example of usage.
