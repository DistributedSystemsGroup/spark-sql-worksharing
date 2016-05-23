name := "spark-sql-tpc-ds-perf"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "apache-snapshots" at "https://repository.apache.org/snapshots/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.0-SNAPSHOT"