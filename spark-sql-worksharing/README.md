|| datum == "null" in TypeCast.scala


`scp /home/ntkhoa/workspace/working/spark-sql-worksharing/spark-sql-worksharing/out/artifacts/spark_sql_worksharing_jar/*.jar spark-master:~`

`/opt/spark/bin/spark-submit --class ComputeStats --master spark://khoa-spark-khoa-master-001:7077 spark-sql-tpc-ds-perf.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G /home/ubuntu/stat.json com.databricks.spark.csv`

`/opt/spark/bin/spark-submit --class App --master spark://khoa-spark-khoa-master-001:7077 spark-sql-tpc-ds-perf.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/output /home/ubuntu/stat.json com.databricks.spark.csv 0 /home/ubuntu/stat.json`