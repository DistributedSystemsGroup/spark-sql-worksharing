name := "spark-sql-worksharing"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.1"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.4"