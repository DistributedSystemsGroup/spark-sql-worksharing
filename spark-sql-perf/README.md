# Spark SQL Performance
This is the library provided by Databricks that supports running TPC-DS queries on Spark SQL.  
The build.sbt is configured to build for Spark at version 2.0.1-SNAPSHOT (newest version)  
Repository URL: <https://github.com/databricks/spark-sql-perf>  
You can get it by:
```
git clone https://github.com/databricks/spark-sql-perf.git
cd spark-sql-perf
// then remove the account credential requirements: dbcUsername, dbcPassword, dbcApiUrl, dbcClusters, dbcLibraryPath in build.sbt
```


### Different sparkVersion build

in Benchmark.scala
- for sparkVersion = 1.6+, check the [commit](https://github.com/databricks/spark-sql-perf/commit/344b31ed69f18205fb8192df2f5a8704e6a62615) 
 `case UnresolvedRelation(t, _) => t.table`
 `tableIdentifier.table`
- for sparkVersion = 1.4, 1.5
 `case UnresolvedRelation(Seq(name), _) => name`
 `tableIdentifier.last`

### Newest spark (version 2.0 snapshot)

in src/main/scala/com/databricks/spark/sql/perf/DatasetPerformance.scala
```
import org.apache.spark.sql.{Encoder, SQLContext}

override def bufferEncoder = implicitly[Encoder[SumAndCount]]
override def outputEncoder = implicitly[Encoder[Double]]
```

### Modification required to support CSV header (only if you need csv header)

```
//1. In Tables.scala, function genData
writer.format(format).option("header", "true").mode(mode)

//2. In Tables.scala, function createTemporaryTable
sqlContext.read.format(format).option("header", "true").option("inferSchema", "true").load(location).registerTempTable(name)

```

### Modification to not do the partitioning by columns
Tables.scala
```
if (clusterByPartitionColumns && partitionColumns.nonEmpty) {
	writer.partitionBy(partitionColumns : _*)
}
```
### How to build?
`./build/sbt package`

### How to use?
- Refer to project spark-sql-tpc-ds-perf for an example of usage.

