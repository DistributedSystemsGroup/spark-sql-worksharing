package org.apache.spark.sql.myExtensions.statistics

import nw.fr.eurecom.dsg.util.Constants
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Column, DataFrame, Row}
import nw.fr.eurecom.dsg.statistics.Util

/**
  * utility object, computing equi-width histograms for all columns of a table
  */
object SparkHistogram{

  private class HistogramCounter(min:Double, max:Double) extends Serializable {
    val nBins: Int = Math.min((max - min + 1).toInt, Constants.NUM_BINS_HISTOGRAM_MAX)
    val buckets = new Array[Long](nBins)
    val binWidth:Double = (max - min + 1) / nBins
    var total:Long = 0 // if we want to output the fraction

    def add(key: Any): this.type = {
      if(key != null){

        var iBucket:Int = 0
        if(key.isInstanceOf[String]){
          val value = Util.stringToInt(key.toString, nBins)
          iBucket= ((value - min.toInt) / binWidth).toInt
        }
        else{
          Util.TryToDouble(key.toString) match{
            case None => {
              val value = Util.stringToInt(key.toString, nBins)
              val iBucket= ((value - min.toInt) / binWidth).toInt
            }
            case Some(v) =>{
              val value = v
              val iBucket= ((value - min) / binWidth).toInt
            }
          }
        }
        buckets(iBucket)+=1
        total+=1
      }

      this
    }

    /**
      * Merge two histogram
      * @param other another histogram computed from another partition
      */
    def merge(other: HistogramCounter): this.type = {
      for(i <- 0 to nBins-1){
        buckets(i) += other.buckets(i)
      }
      this
    }
  }

  def singlePassHistogramCounter( df: DataFrame,
                                  mins:Array[Double],
                                  maxs: Array[Double]): DataFrame = {
    val cols = df.columns.toSeq
    val numCols = cols.length

    val histMaps = Seq.tabulate(numCols)(i => new HistogramCounter(mins(i), maxs(i)))
    val originalSchema = df.schema
    val colInfo: Array[(String, DataType)] = cols.map { name =>
      val index = originalSchema.fieldIndex(name)
      (name, originalSchema.fields(index).dataType)
    }.toArray

    val histCounters = df.select(cols.map(Column(_)) : _*).rdd.aggregate(histMaps)(
      seqOp = (hist, row) => {
        var i = 0
        while (i < numCols) {// foreach column
          val thisMap = hist(i)
          val key = row.get(i)
          thisMap.add(key)
          i += 1
        }
        hist
      },
      combOp = (baseHists, hists) => {
        var i = 0
        while (i < numCols) {
          baseHists(i).merge(hists(i))
          i += 1
        }
        baseHists
      }
    )
    val histItems = histCounters.map(m => m.buckets)
    val resultRow = Row(histItems : _*)

    val outputCols = colInfo.map { v =>
      StructField(v._1 + "_hist", ArrayType(LongType, false))
    }
    val schema = StructType(outputCols).toAttributes
    Dataset.ofRows(df.sparkSession, LocalRelation.fromExternalRows(schema, Seq(resultRow)))
  }
}
