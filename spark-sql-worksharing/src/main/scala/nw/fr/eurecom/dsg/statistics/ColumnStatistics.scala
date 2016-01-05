package nw.fr.eurecom.dsg.statistics

import com.fasterxml.jackson.annotation.JsonCreator
import nw.fr.eurecom.dsg.util.Constants
import org.apache.spark.sql.catalyst.expressions.Expression

/**
  * Holds statistics information of a column
  * The histBuckets is an equi-width histogram
  * @param numNotNull
  * @param numNull
  * @param mean
  * @param stddev
  * @param min
  * @param max
  * @param numDistincts
  * @param histBuckets
  */
@JsonCreator
class ColumnStatistics(val numNotNull: Long = Constants.UNKNOWN_VAL,
                       val numNull: Long = Constants.UNKNOWN_VAL,
                       val mean: Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val stddev: Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val min: Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val max: Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val numDistincts: Long = Constants.UNKNOWN_VAL,
                       var histBuckets: Array[Long] = null) {
  val nBins: Int = Math.min((max - min + 1).toInt, Constants.NUM_BINS_HISTOGRAM_MAX)

  // holds number of items in each bucket
  // note that the histogram is equi-width
  var buckets: Array[Long] = null

  if (histBuckets != null)
    buckets = histBuckets
  else
    buckets = new Array[Long](nBins)

  val binWidth: Double = (max - min + 1) / nBins // mod >= 1

  def putHistogramData(data: Array[Long]): Unit = {
    for (i <- 0 to nBins - 1) {
      buckets(i) = data(i)
    }
  }

  // HistogramCounter in SparkHistogram.scala
  //  def add(key: Any): this.type = {
  //    if(key != null){
  //      if(key.isInstanceOf[String]){
  //        val value = Util.stringToInt(key.toString, nBins)
  //        val iBucket= ((value - min.toInt) / mod).toInt
  //        buckets(iBucket)+=1
  //
  //      }
  //      else{
  //        val value = key.toString.toDouble
  //        val iBucket= ((value - min) / mod).toInt
  //        buckets(iBucket)+=1
  //      }
  //      total+=1
  //    }
  //    this
  //  }

  def getMinimumEstimation(from:Any):Double={
    getRangeEstimation(from = from, to = this.max) - getEqualityEstimation(from)
  }

  def getMaximumEstimation(to:Any):Double={
    getRangeEstimation(from = this.min, to = to) - getEqualityEstimation(to)
  }

  /**
    * [from, to]
    * @param from
    * @param to
    */
  def getRangeEstimation(from: Any, to: Any): Double = {
    var valueFrom: Double = 0
    var valueTo: Double = 0

    if (from.isInstanceOf[String] && to.isInstanceOf[String]) {
      valueFrom = Util.stringToInt(from.toString, nBins)
      valueTo = Util.stringToInt(to.toString, nBins)
    }
    else {
      valueFrom = from.toString.toDouble
      valueTo = to.toString.toDouble
    }

    if (valueFrom < min) valueFrom = min.toInt
    if (valueTo > max) valueTo = max.toInt
    val iBucketFromDouble = (valueFrom - min.toInt) / binWidth
    val iBucketFrom = iBucketFromDouble.toInt

    val iBucketToDouble = (valueTo - min.toInt) / binWidth
    val iBucketTo = iBucketToDouble.toInt

    val estimationForBinFrom = buckets(iBucketFrom) * (1 - (iBucketFromDouble - iBucketFrom))
    val estimationForBinTo = buckets(iBucketTo) * (iBucketToDouble - iBucketTo)
    var res = (estimationForBinFrom + estimationForBinTo).toLong

    for (i <- iBucketFrom + 1 to iBucketTo - 1) {
      res += buckets(i)
    }
    res * 1.0 / (numNull + numNotNull)
  }


  /**
    * provides estimation for the following cases:
    * - column == key
    * - column like key
    * @param key
    * @return number of records satisfying the equality condition
    */
  def getEqualityEstimation(key: Any): Double = {
    if (key == null || key == "null") return numNull*1.0 / (numNull + numNotNull)

    var value: Double = 0

    if (key.isInstanceOf[String])
      value = Util.stringToInt(key.toString, nBins)
    else
      value = key.toString.toDouble

    if (value < min || value > max) return 0
    val iBucket = ((value - min.toInt) / binWidth).toInt
    (buckets(iBucket)*1.0 / Math.max(binWidth, 1)) / (numNull + numNotNull)
  }

}