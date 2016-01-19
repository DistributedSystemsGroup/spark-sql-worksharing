package nw.fr.eurecom.dsg.statistics

import java.io._
import java.util.concurrent.atomic.AtomicLong
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import nw.fr.eurecom.dsg.util.{SparkSQLServerLogging, Constants, QueryProvider}
import org.apache.spark.scheduler._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.myExtensions.statistics.{SparkHistogram, BasicStatGatherer}
import org.apache.spark.sql.types._
import scala.collection.mutable
import scala.collection.mutable.HashMap
import org.apache.spark.sql.catalyst.expressions.Expression

/**
  * Hold statistics data of all base relations (base tables)
  * We have 2 levels: Table statistics (see RelationStatistics class)
  * & column statistics (see ColumnStatistics class)
  *
  * Entry point for asking statistics data
  */
class StatisticsProvider extends SparkSQLServerLogging{

  // Map a table to its RelationStatistics
  val baseRelationStats = new HashMap[String, RelationStatistics]()

  def getTableStatistics(tableName:String):RelationStatistics={
    baseRelationStats.get(tableName).get
  }

  /**
    * collect/compute statistics data for each table
    * For each table, 2 jobs are submitted
    * - the first computes basic stats: count, min, max, cardinality,...
    * - the second computes Histogram for each column (using the min, max of each column obtained)
    * @param tables tables to compute statistics data
    * @param queryProvider
    */
  def collect(tables: Seq[String], queryProvider: QueryProvider){

    // Obtaining statistics information from built-in SQL DataFrame
    // Spark SQL currently supports basic statistics for numeric columns
    // Ref: https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
    // We can obtain: [(column_name, (count, mean, stddev, min, max))] from it
    // Customized version:
    // + add ApproxCountDistinct for each column (cardinality estimation with the HyperLogLog algorithm)
    // + compute Histogram for each column
    // TODO: How about using dataframe.stat.corr('column1', 'column2')? to compute the statistical dependence between 2 columns?

    val inputSizeAtomic:AtomicLong = new AtomicLong(0)
    val numRecordsAtomic:AtomicLong = new AtomicLong(0)
    val numSplitsAtomic:AtomicLong = new AtomicLong(0)

    def resetCounter(): Unit ={
      inputSizeAtomic.set(0); numRecordsAtomic.set(0); numSplitsAtomic.set(0)
    }

    // A call-back function whenever a ShuffleMapTask is completed
    queryProvider.sqlContext.sparkContext.addSparkListener(new StatsReportListener(){
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd){
        if(taskEnd.taskType == "ShuffleMapTask"){ // Check the query below for more information
          val info = taskEnd.taskInfo
          val metrics = taskEnd.taskMetrics
          if (info != null && metrics != null) {
            if(metrics.inputMetrics.isDefined){
              inputSizeAtomic.getAndAdd(metrics.inputMetrics.get.bytesRead)
              numRecordsAtomic.getAndAdd(metrics.inputMetrics.get.recordsRead)
              numSplitsAtomic.getAndIncrement()
            }
          }
        }
      }
    })

    tables.foreach(tableName => {
      // TODO: any different way?
      resetCounter()
      val tableDF = queryProvider.getDF("SELECT * FROM " + tableName)
      val builtinStats = BasicStatGatherer.execute(queryProvider.sqlContext, tableDF).collect()
      // Note that Spark will generate 2 jobs for this query. The first job only read 10 first lines to parse the header (we can safely ignore this)
      // We will update the inputSizeAtomic, numRecordsAtomic, numSplitsAtomic based on the second job
      // Result: Row[6]
      // Row[0]: count (not null)
      // Row[1]: mean // removed, DateType not possible
      // Row[2]: stddev // removed, DateType not possible
      // Row[1]: min
      // Row[2]: max
      // Row[3]: ApproxCountDistinct

      builtinStats.foreach(println)

      def getCellFromBuiltinStats(row:Int, col:Int):Any={
        builtinStats(row).asInstanceOf[GenericRowWithSchema].get(col)
      }

      val inputSize:Long = inputSizeAtomic.get()
      val numRecords:Long = numRecordsAtomic.get() - 1 // - header
      val numSplits:Long = numSplitsAtomic.get()

      println("Table %s: InputSize=%d, NumRecords=%d, NumSplits=%d".format(tableName, inputSize, numRecords, numSplits))

      val columns = tableDF.schema.map(f => (f.name, f.dataType))
      val nColums = tableDF.columns.length
      val columnsStats = new HashMap[String, ColumnStatistics]()

      // useful for computing histogram then
      val mins = new Array[Double](nColums)
      val maxs = new Array[Double](nColums)

      // Read the result for each column
      // Initialize a ColumnStatistics for each column and add it to columnsStats
      columns.indices.foreach{i =>
        val columnName = columns(i)._1
        val dataType = columns(i)._2

        println(columnName + "-" + dataType)

        val numNotNull = getCellFromBuiltinStats(0, i+1).toString.toLong
        val numNull = numRecords - numNotNull
        var mean:Double = Constants.UNKNOWN_VAL_DOUBLE // default value
        var stddev:Double = Constants.UNKNOWN_VAL_DOUBLE // default value
        var min:Double = Constants.MIN_STRING_TYPE
        var max:Double = Constants.MAX_STRING_TYPE
        val numDistincts:Long = getCellFromBuiltinStats(3, i+1).toString.toLong

        dataType match {
          case dt:IntegerType =>
            //mean = getCellFromBuiltinStats(1, i+1).toString.toDouble
            //stddev = getCellFromBuiltinStats(2, i+1).toString.toDouble

            min = getCellFromBuiltinStats(1, i+1) match{
              case null => 0
              case v:Any => v.toString.toInt
            }
            max = getCellFromBuiltinStats(2, i+1) match{
              case null => 0
              case v:Any => v.toString.toInt
            }

          case dt:LongType =>
            //mean = getCellFromBuiltinStats(1, i+1).toString.toDouble
            //stddev = getCellFromBuiltinStats(2, i+1).toString.toDouble
            min = getCellFromBuiltinStats(1, i+1) match{
              case null => 0
              case v:Any => v.toString.toLong
            }
            max = getCellFromBuiltinStats(2, i+1) match{
              case null => 0
              case v:Any => v.toString.toLong
            }
          case dt:DecimalType =>
            //mean = getCellFromBuiltinStats(1, i+1).toString.toDouble
            //stddev = getCellFromBuiltinStats(2, i+1).toString.toDouble
            min = getCellFromBuiltinStats(1, i+1) match{
              case null => 0
              case v:Any => v.toString.toDouble
            }
            max = getCellFromBuiltinStats(2, i+1) match{
              case null => 0
              case v:Any => v.toString.toDouble
            }

          // Don't get the avg, stddev,min,max for the following types
          case dt:DateType =>
            if(numDistincts < Constants.NUM_BINS_HISTOGRAM_MAX)
              max = numDistincts.toInt
          case dt:StringType =>
            if(numDistincts < Constants.NUM_BINS_HISTOGRAM_MAX)
              max = numDistincts.toInt
        }

        mins(i) = min
        maxs(i) = max
        val colStats = new ColumnStatistics(numNotNull, numNull,mean, stddev, min, max, numDistincts)

        columnsStats.put(columnName, colStats)
      }

      // Compute the histogram for each column
      val histogramRes = SparkHistogram.singlePassHistogramCounter(tableDF, mins, maxs).collect()
      // Result: Row[1]

      def extractHist(col:Int):Array[Long]={
        histogramRes(0).asInstanceOf[GenericRowWithSchema].get(col).asInstanceOf[mutable.WrappedArray[Long]].toArray
      }

      columns.indices.foreach(i =>{
        val columnName = columns(i)._1
        columnsStats.get(columnName).get.putHistogramData(extractHist(i))
      })

      val relationStat = new RelationStatistics(inputSize = inputSize,
        numRecords = numRecords,
        averageRecSize = inputSize*1.0 / numRecords,
        columnStats = columnsStats)

      baseRelationStats.put(tableName, relationStat)
    })

  }

  /**
    * Save statistics data collected to a json file
    * @param file
    */
  def saveToFile(file:String)={
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new FileOutputStream(file)
    mapper.writeValue(out, this)
  }

  /**
    * Read statistics data from a json file
    * @param file
    * @return
    */
  def readFromFile(file:String):StatisticsProvider={
    val json = scala.io.Source.fromFile(file).mkString
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val res = mapper.readValue(json, classOf[StatisticsProvider])
    res

  }

  def estimateExpression(exp:Expression): Unit ={

  }
}
