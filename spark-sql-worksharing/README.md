## Idea
- Group all queries to a central server for optimizing them in batch
- Find the common (similar) subexpressions (CSE)
- Transform the original queries 


## Architecture:
- Step 1: Precompute some basic statistics and materialize it to disk  
`ComputeStats <master> <inputDir> <savePath> <format>`
    + format = {parquet, csv}

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
        
### Micro-benchmark - generating data
resources/generator.py  
generate random data for the micro-benchmark  
schema: n1 -> n10, d1->d10, s1->s10  
``python generator.py 1000000 | hdfs dfs -put - tmp/random-data.txt``

### Micro-benchmark











`scp /home/ntkhoa/workspace/working/spark-sql-worksharing/spark-sql-worksharing/out/artifacts/spark_sql_worksharing_jar/*.jar spark-master:~`

`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.ComputeStats --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G /home/ubuntu/stat_10G.json com.databricks.spark.csv`
`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.ComputeStats --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_50G /home/ubuntu/stat_50G.json com.databricks.spark.csv`


`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.AppTmp --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/output_csv_10G com.databricks.spark.csv 0 /home/ubuntu/stat_10G.json`


## Issues & resolution
- datetime datatype problem:
fix: when generating, change to string
when read: string too. among 99 tpc-ds queries, none of them uses those date columns 

- Table call_center, the 5th column "cc_closed_date_sk", datatype:int, all records = null!!!
- 

- Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main": give Spark more memory to initialize HiveContext.
    + in local: set `-XX:MaxPermSize=2G` (maximum size of Permanent Generation) in VM Options
    + in cluster: `--driver-java-options -XX:MaxPermSize=2G` 
    
    
cannot parsed:
16, 23a, 32, 41, 92, 95

benchmark fail: 9
16, 23a, 32, 41, 92, 95