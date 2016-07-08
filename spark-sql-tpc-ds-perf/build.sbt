name := "spark-sql-tpc-ds-perf"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.4"

//TODO: to be removed after spark 2.0 is released
resolvers += "apache-snapshots" at "https://repository.apache.org/snapshots/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.1-SNAPSHOT"

