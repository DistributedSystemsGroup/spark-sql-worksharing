// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

scalaVersion := "2.10.4"

sparkPackageName := "databricks/spark-sql-perf"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

sparkVersion := "1.6.1"

resolvers += "Apache Preview Repo" at "https://repository.apache.org/content/repositories/orgapachespark-1156/"

sparkComponents ++= Seq("sql", "hive")

initialCommands in console :=
  """
    |import org.apache.spark.sql.hive.test.TestHive
    |import TestHive.implicits
    |import TestHive.sql
  """.stripMargin

libraryDependencies += "com.twitter" %% "util-jvm" % "6.23.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

import ReleaseTransformations._

/** Push to the team directory instead of the user's homedir for releases. */
lazy val setupDbcRelease = ReleaseStep(
  action = { st: State =>
    val extracted = Project.extract(st)
    val newSettings = extracted.structure.allProjectRefs.map { ref =>
      dbcLibraryPath in ref := "/databricks/spark/sql/lib"
    }

    reapply(newSettings, st)
  }
)

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  setupDbcRelease,
  releaseStepTask(dbcUpload),
  setNextVersion,
  commitNextVersion,
  pushChanges
)
