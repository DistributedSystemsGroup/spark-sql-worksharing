# Spark SQL TPC-DS performance benchmark

## Dependencies
- spark-hive (in build.sbt): to be able to use the HiveContext instead of SparkContext in generating the database
`libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.2"`
- spark-sql-perf (in lib/): from DataBrick [Github url](https://github.com/databricks/spark-sql-perf). Clone version is included in this repository (/spark-sql-perf)
`cp ../spark-sql-perf/target/scala-2.10/*.jar lib/`
- tpc-ds-tool/ : folder containing official tpc-ds tool to generate data
- spark-csv & commons-csv in lib/

## Utilities

### GenApp: application to initialize dataset for the benchmark
- arguments: <inputPath> <scaleFactor> <format> <dsdgenDir>
    + scaleFactor: number of GB of data to be generated, eg: 10, 100, ...
    + format: [parquet, com.databricks.spark.csv]
    + dsdgenDir: folder tpc-ds-tool/
    
### BenchmarkApp: the application for doing benchmarking the TPC-DS 1.4 queries

## Issues & resolve:
- Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main": give Spark more memory to initialize HiveContext.
    + in local mode : set -XX:MaxPermSize=2G (maximum size of Permanent Generation) in VM Options
    + otherwise: --driver-java-options -XX:MaxPermSize=2G 
- CSV with no headers: 
    + add `writer.format(format).option("header", "true").mode(mode)` in Tables.scala, function genData
    + ```sqlContext.read.format(format)
              .option("header", "true") // Use first line of all files as header
              .option("inferSchema", "true") // Automatically infer data types
              .load(location).registerTempTable(name)``` 
    in Tables.scala, function createTemporaryTable 


## Additional information for Eurecom internal users
- Copy the tpc-ds-tool to each worker machine, same location
`scp -r tpc-ds-tool spark-worker1:~`
- setup on master to get access to the WebUI
```
ssh -X spark-master
sudo apt-get install firefox
firefox &
```
- submission: 
Copy jar: `scp /home/ntkhoa/workspace/working/spark-sql-worksharing/spark-sql-tpc-ds-perf/out/artifacts/spark_sql_tpc_ds_perf_jar/*.jar spark-master:~`
`/opt/spark/bin/spark-submit --class GenApp --master spark://khoa-spark-khoa-master-001:7077 spark-sql-tpc-ds-perf.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_parquet_10G 10 parquet ~/tpc-ds-tool`
`/opt/spark/bin/spark-submit --class GenApp --master spark://khoa-spark-khoa-master-001:7077 spark-sql-tpc-ds-perf.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G 10 com.databricks.spark.csv ~/tpc-ds-tool`

- org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException): Permission denied...
```
sudo groupadd supergroup
sudo usermod -a -G supergroup ubuntu
```

#### .ssh/config
```
Host spark-master
  HostName 192.168.45.216
  User ubuntu
  IdentityFile ~/.ssh/bigfoot

Host spark-worker1
  HostName 192.168.45.217
  User ubuntu
  IdentityFile ~/.ssh/bigfoot

Host spark-worker2
  HostName 192.168.45.218
  User ubuntu
  IdentityFile ~/.ssh/bigfoot

Host spark-worker3
  HostName 192.168.45.22
  User ubuntu
  IdentityFile ~/.ssh/bigfoot

Host spark-worker4
  HostName 192.168.45.220
  User ubuntu
  IdentityFile ~/.ssh/bigfoot

Host spark-worker5
  HostName 192.168.45.221
  User ubuntu
  IdentityFile ~/.ssh/bigfoot

Host spark-worker6
  HostName 192.168.45.222
  User ubuntu
  IdentityFile ~/.ssh/bigfoot
```