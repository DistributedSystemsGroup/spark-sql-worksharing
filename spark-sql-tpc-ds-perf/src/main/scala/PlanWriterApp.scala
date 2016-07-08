import java.io.{BufferedWriter, File, FileWriter}
import java.lang.Exception

import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import util.{QueryProvider, Tables}

import scala.collection.mutable.HashMap

object PlanWriterApp {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: <inputPath> <format>")
      // scaleFactor: number of GB of data to be generated
      System.exit(-1)
    }
    val inputPath = args(0)
    val format = args(1)

    val conf = new SparkConf().setAppName(this.getClass.getName())
    conf.set("spark.sql.perf.results", args(1))

    conf.setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    // You need the HiveContext to be able to fully parse the queries
    val sqlContext = new HiveContext(sparkContext)

    val tables = Tables.getAllTables()
    val queryProvider = new QueryProvider(sqlContext, inputPath, tables, format)

    val tpcds = new TPCDS(sqlContext = sqlContext)

    val queries = tpcds.tpcds1_4Queries

    def tryNewDF(q: Query): (String, LogicalPlan, String) = {
      try {
        (q.name, q.newDataFrame().queryExecution.optimizedPlan, q.sqlText.get)
      }
      catch {
        case e:Exception =>
          println("Error: query " + q.name + " " + e.toString)
          (q.name, null, q.sqlText.get)
      }
    }

    val dfs = queries.map(q => tryNewDF(q))

    def writeLPToTextFile(lp_queryText: (String, LogicalPlan, String)) {
      if (lp_queryText != null) {
        val name = lp_queryText._1
        val lp = lp_queryText._2
        val queryText = lp_queryText._3

        try {
          val file = new File(name + ".txt")
          val bw = new BufferedWriter(new FileWriter(file))
          bw.write(queryText + "\n")
          bw.write(lp.toString() + "\n")
          bw.write(getVisualizedString(lp))
          bw.close()
        }
        catch {
          case e:Exception => println("Error: query " + lp_queryText._1 + " " + e.toString)
        }
      }
    }
    dfs.foreach(lp => writeLPToTextFile(lp._1, lp._2, lp._3))
  }

  def getVisualizedString(p: LogicalPlan): String = {
    val opName = p.getClass.getSimpleName

    p match {
      case b: BinaryNode =>
        return "[%s %s %s]".format(opName, getVisualizedString(b.left), getVisualizedString(b.right))
      case u: UnaryNode =>
        return "[%s %s]".format(opName, getVisualizedString(u.child))
      case l: LeafNode =>
        val path = l.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].inputFiles.mkString(" ")
        return "%s".format(path.substring(path.lastIndexOf('/') + 1))
      case u: Union =>
        var res = opName
        u.children.foreach(c => res = res + " " + getVisualizedString(c))
        return "[" + res + "]"
    }
    ""
  }
}
