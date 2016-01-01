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
class ColumnStatistics(val numNotNull:Long = Constants.UNKNOWN_VAL,
                       val numNull:Long = Constants.UNKNOWN_VAL,
                       val mean:Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val stddev:Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val min:Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val max:Double = Constants.UNKNOWN_VAL_DOUBLE,
                       val numDistincts:Long = Constants.UNKNOWN_VAL,
                       var histBuckets:Array[Long]= null){
  val nBins: Int = Math.min((max - min + 1).toInt, Constants.NUM_BINS_HISTOGRAM_MAX)

  // holds number of items in each bucket
  // note that the histogram is equi-width
  var buckets:Array[Long] = null

  if(histBuckets != null)
    buckets = histBuckets
  else
    buckets = new Array[Long](nBins)

  val mod:Double = (max - min + 1) / nBins

  def putHistogramData(data:Array[Long] ): Unit ={
    for(i <- 0 to nBins-1){
      buckets(i) = data(i)
    }
  }

  def estimate(expression: Expression):Double={
    0
  }

}