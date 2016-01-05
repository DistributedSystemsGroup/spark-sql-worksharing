|| datum == "null" in TypeCast.scala

- Table call_center, 5th column "cc_closed_date_sk", datatype:int, all records = null!!!


`scp /home/ntkhoa/workspace/working/spark-sql-worksharing/spark-sql-worksharing/out/artifacts/spark_sql_worksharing_jar/*.jar spark-master:~`

`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.ComputeStats --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G /home/ubuntu/stat_10G.json com.databricks.spark.csv`
`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.ComputeStats --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_50G /home/ubuntu/stat_50G.json com.databricks.spark.csv`


`/opt/spark/bin/spark-submit --class nw.fr.eurecom.dsg.AppTmp --master spark://khoa-spark-khoa-master-001:7077 spark-sql-worksharing.jar hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/input_csv_10G hdfs://khoa-spark-khoa-master-001:8020/user/ubuntu/output_csv_10G com.databricks.spark.csv 0 /home/ubuntu/stat_10G.json`

