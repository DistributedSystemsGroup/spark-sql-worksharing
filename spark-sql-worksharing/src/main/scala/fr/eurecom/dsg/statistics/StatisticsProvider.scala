package fr.eurecom.dsg.statistics

import java.io._
import java.util.concurrent.atomic.AtomicLong
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import fr.eurecom.dsg.util.{SparkSQLServerLogging, Constants, QueryProvider}
import org.apache.spark.scheduler._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.extensions.statistics.{HistogramAggregator, BasicStatGatherer}
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
class StatisticsProvider{

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
 *
    * @param tables tables to compute statistics data
    * @param queryProvider
    */
  def collect(tables: Seq[String], queryProvider: QueryProvider){
    // atomic variables
    val inputSizeAtomic:AtomicLong = new AtomicLong(0)
    val numRecordsAtomic:AtomicLong = new AtomicLong(0)
    val numSplitsAtomic:AtomicLong = new AtomicLong(0) // at the moment, we have not used this info yet

    def resetCounter(): Unit ={ inputSizeAtomic.set(0); numRecordsAtomic.set(0); numSplitsAtomic.set(0) }

    // A call-back function whenever a ShuffleMapTask is completed
    queryProvider.sqlContext.sparkContext.addSparkListener(new StatsReportListener(){
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd){
        if(taskEnd.taskType == "ShuffleMapTask"){ // Check the query below for more information
          val info = taskEnd.taskInfo
          val metrics = taskEnd.taskMetrics
          if (info != null && metrics != null) {
              inputSizeAtomic.getAndAdd(metrics.inputMetrics.bytesRead)
              numRecordsAtomic.getAndAdd(metrics.inputMetrics.recordsRead)
              numSplitsAtomic.getAndIncrement()
          }
        }
      }
    })

    tables.foreach(tableName => {
      resetCounter()
      val tableDF = queryProvider.getDF("SELECT * FROM " + tableName)
      val builtinStats = BasicStatGatherer.execute(tableDF).collect()
      // Result: Row[4]
      // Row[0]: count (not null)
      // Row[1]: min
      // Row[2]: max
      // Row[3]: ApproxCountDistinct

      def getCellFromBuiltinStats(row:Int, col:Int):Any={
        builtinStats(row).asInstanceOf[GenericRowWithSchema].get(col)
      }

      val inputSize:Long = inputSizeAtomic.get()
      val numRecords:Long = numRecordsAtomic.get()
      val numSplits:Long = numSplitsAtomic.get()

      val columns = tableDF.schema.map(f => (f.name, f.dataType))
      val nColums = tableDF.columns.length
      val columnsStats = new HashMap[String, ColumnStatistics]()

      // useful for computing histogram then
      val mins = new Array[Double](nColums)
      val maxs = new Array[Double](nColums)
      val nBins = new Array[Int](nColums)

      // Read the result for each column
      // Initialize a ColumnStatistics for each column and add it to columnsStats
      columns.indices.foreach{i =>
        val columnName = columns(i)._1
        val dataType = columns(i)._2

        val numNotNull = getCellFromBuiltinStats(0, i+1).toString.toLong
        val numNull = numRecords - numNotNull
        var min:Double = Constants.DEFAULT_MIN
        var max:Double = Constants.DEFAULT_MAX
        var numDistinct:Long = getCellFromBuiltinStats(3, i+1).toString.toLong
        var numBins:Int = 1

        dataType match {
            // get min, max for the following types

          case _:BooleanType | _:BinaryType => {
            min = 0
            max = 1
            numDistinct = 2
            numBins = 2
          }

          case _:NumericType =>{
            min = getCellFromBuiltinStats(1, i+1) match{
              case null => 0
              case v:Any => v.toString.toDouble
            }
            max = getCellFromBuiltinStats(2, i+1) match{
              case null => 0
              case v:Any => v.toString.toDouble
            }

            numBins = Math.min(numDistinct.toInt, Constants.NUM_BINS_HISTOGRAM_MAX)
          }

          // Don't get the min,max for the following types
          case _:StringType | _:DateType | _:TimestampType =>
            numBins = Math.min(numDistinct.toInt, Constants.NUM_BINS_HISTOGRAM_MAX)
        }

        mins(i) = min
        maxs(i) = max
        nBins(i) = numBins
        val colStats = new ColumnStatistics(numNotNull, numNull, min, max, numDistinct)

        columnsStats.put(columnName, colStats)
      }

      //***************************************
      // Compute the histogram for each column
      //***************************************
      val histogramRes = HistogramAggregator.execute(tableDF, mins, maxs, nBins).collect()
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
      println("================== STATISTICS for Table %s ==================".format(tableName))
      println("NumSplits=%d".format(numSplits))
      println(relationStat.toString())

    })

  }

  /**
    * Save statistics data collected to a json file
 *
    * @param file
    */
  def saveToFile(file:String)={
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new FileOutputStream(file, false)
    mapper.writeValue(out, this)
  }

  /**
    * Read statistics data from a json file
 *
    * @param file
    * @return
    */
  def readFromFile(file:String):StatisticsProvider={
    val json = scala.io.Source.fromFile(file).mkString
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val res = mapper.readValue(json, classOf[StatisticsProvider])
    res

  }

  def estimateExpression(exp:Expression): Unit ={

  }
}
