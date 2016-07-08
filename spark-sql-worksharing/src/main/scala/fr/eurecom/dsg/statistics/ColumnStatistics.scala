package fr.eurecom.dsg.statistics

import com.fasterxml.jackson.annotation.JsonCreator
import fr.eurecom.dsg.util.Constants
import org.apache.spark.sql.extensions.Util
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.Exception

/**
  * POJO class
  * Holds statistics information of a column
  * The histBuckets is an equi-width histogram
 *
  * @param numNotNull
  * @param numNull
  * @param min
  * @param max
  * @param numDistincts
  * @param histBuckets
  */
@JsonCreator
class ColumnStatistics(val numNotNull: Long = Constants.UNKNOWN_VAL,
                       val numNull: Long = Constants.UNKNOWN_VAL,
                       val min: Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val max: Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val numDistincts: Long = Constants.UNKNOWN_VAL,
                       var histBuckets: Array[Long] = Array.empty) {

  override def toString()={
    "NumNotNull=%d, NumNull=%d, Min=%f, Max=%f, NumDistinct=%d, Buckets=%s".format(numNotNull, numNull, min, max, numDistincts, histBuckets.mkString(", "))
  }

  def nBins = histBuckets.length

  def binWidth = (max - min) / nBins

  def putHistogramData(data: Array[Long]): Unit = {
    histBuckets = data
  }


  //************************************************
  // ESTIMATION FUNCTIONS
  //************************************************

  def getGreaterThanEstimation(from:Any):Double={
    getRangeEstimation(from = from, to = this.max) - getEqualityEstimation(from)
  }

  def getLessThanEstimation(to:Any):Double={
    getRangeEstimation(from = this.min, to = to) - getEqualityEstimation(to)
  }

  def getUniquenessDegree():Double = {
      numDistincts*1.0 / numNotNull
  }

  /**
    * [from, to]
 *
    * @param from
    * @param to
    */
  def getRangeEstimation(from: Any, to: Any): Double = {
    var valueFrom: Double = 0
    var valueTo: Double = 0

    try{
      valueFrom = from.toString.toDouble
      valueTo = to.toString.toDouble
    }
    catch {
      case _ =>
        valueFrom = Util.stringToInt(from.toString, nBins)
        valueTo = Util.stringToInt(to.toString, nBins)
    }

    if (valueFrom < min) valueFrom = min.toInt
    if (valueTo > max) valueTo = max.toInt
    val iBucketFromDouble = (valueFrom - min.toInt) / binWidth
    val iBucketFrom = Math.max(0, Math.min(nBins-1, iBucketFromDouble.toInt))

    val iBucketToDouble = (valueTo - min.toInt) / binWidth
    val iBucketTo = Math.max(0, Math.min(nBins-1, iBucketToDouble.toInt))

    val estimationForBinFrom = histBuckets(iBucketFrom) * (1 - (iBucketFromDouble - iBucketFrom))
    val estimationForBinTo = histBuckets(iBucketTo) * (iBucketToDouble - iBucketTo)
    var res = (estimationForBinFrom + estimationForBinTo).toLong

    for (i <- iBucketFrom + 1 to iBucketTo - 1) {
      res += histBuckets(i)
    }
    res * 1.0 / (numNull + numNotNull)
  }


  /**
    * provides estimation for the following cases:
    * - column == key
    * - column like key
 *
    * @param key
    * @return number of records satisfying the equality condition
    */
  def getEqualityEstimation(key: Any): Double = {
    if (key == null || key == "null") return numNull*1.0 / (numNull + numNotNull)

    var value: Double = 0

    if (key.isInstanceOf[String] || key.isInstanceOf[UTF8String])
      value = Util.stringToInt(key.toString, nBins)
    else
      value = key.toString.toDouble

    if (value < min || value > max) return 0
    val iBucket = Math.max(0, Math.min(((value - min.toInt) / binWidth).toInt, nBins - 1))
    (histBuckets(iBucket)*1.0 / Math.max(binWidth, 1)) / (numNull + numNotNull)
  }

}