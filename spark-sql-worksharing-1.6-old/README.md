## Idea
- Group all queries to a central server for optimizing them in batch
- Find the common (similar) subexpressions (CSE)
- Transform the original queries 


## Architecture:
- Step 1: Precompute some basic statistics and materialize it to disk  
`ComputeStats <master> <inputDir> <savePath> <format>`
    + format = {parquet, com.databricks.spark.csv}

- 




### Precomputed statistics
- check class `StatisticsProvider`. Method `collect(tables)` will start computing stats for given list of tables. Method `saveToFile`, `readFromFile`  
- Statistics are of 2 levels: 
    + Relation level (table level) (class `RelationStatistics`)
        * input size
        * num records
        * averageRecSize
        * column stats
    + Column level (class `ColumnStatistics`)
        * num records not null
        * num null records
        * mean (if applicable)
        * stddev (if applicable)
        * min, max (if applicable)
        * count distinct
        * histograms (equal-width histograms)
        
    










`scp /home/ntkhoa/workspace/working/spark-sql-worksharing/spark-sql-worksharing/out/artifacts/spark_sql_worksharing_jar/*.jar spark-master:~`

`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.ComputeStats --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G /home/ubuntu/stat_10G.json com.databricks.spark.csv`
`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.ComputeStats --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_50G /home/ubuntu/stat_50G.json com.databricks.spark.csv`


`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.AppTmp --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/output_csv_10G com.databricks.spark.csv 0 /home/ubuntu/stat_10G.json`


## Issues & resolution
- Table call_center, the 5th column "cc_closed_date_sk", datatype:int, all records = null!!!
- Solution: build your own spark-csv version
- `|| datum == "null"` in TypeCast.scala

- Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main": give Spark more memory to initialize HiveContext.
    + in local: set `-XX:MaxPermSize=2G` (maximum size of Permanent Generation) in VM Options
    + in cluster: `--driver-java-options -XX:MaxPermSize=2G` 