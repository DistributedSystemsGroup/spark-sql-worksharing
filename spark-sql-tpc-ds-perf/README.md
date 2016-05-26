# Spark SQL TPC-DS performance benchmark

## Dependencies
- spark-hive (in build.sbt): to be able to use the HiveContext instead of SparkContext in generating the database
- spark-sql-perf (in lib/): from DataBrick [Github url](https://github.com/databricks/spark-sql-perf). Cloned version is included in this repository (/spark-sql-perf)
`cp ../spark-sql-perf/target/scala-2.10/*.jar lib/`
- tpc-ds-tool/ : folder containing official tpc-ds tool to generate data
- old versions (prior to 1.6.x) of spark require `spark-csv` & `commons-csv` library to be able to read and write csv files.

## How to use?

### GenApp: 
Application to generate the dataset for the benchmark
- arguments: <inputPath> <scaleFactor> <format> <dsdgenDir>
    + master: {local, cluster}
    + outputPath: where to save the generated data
    + scaleFactor: number of GB of data to be generated, eg: 10, 100, ...
    + format: {parquet, csv}
    + dsdgenDir: folder tpc-ds-tool/
- Example: local /home/ntkhoa/tpcds-csv 1 csv tpc-ds-tool
- Example: local /home/ntkhoa/tpcds-parquet 1 parquet tpc-ds-tool
    
### BenchmarkApp: 
Application for doing benchmarking the TPC-DS 1.4 queries
- arguments: <inputPath> <outputPath> <scaleFactor> <format> <iterations> <dsdgenDir>
- Example: /home/ntkhoa/tpcds-csv /home/ntkhoa/out 1 csv 1 tpc-ds-tool

## Issues & resolve:
- Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main": give Spark more memory to initialize HiveContext.
    + in local: set `-XX:MaxPermSize=2G` (maximum size of Permanent Generation) in VM Options
    + in cluster: `--driver-java-options -XX:MaxPermSize=2G` 
- CSV with no headers: read README in project `spark-sql-perf`

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

cannot parsed: 16, 23a, 32, 41, 92, 95
cannot benchmarked: 9

