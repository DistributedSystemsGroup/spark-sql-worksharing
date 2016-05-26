package org.apache.spark.sql.extensions.statistics

import org.apache.spark.sql.extensions.Util
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}

/**
  * utility object, computing equi-width histograms for all columns of a table
  */
object HistogramAggregator{

  private class Histogram(min:Double, max:Double, nBins:Int) extends Serializable {
    val buckets = new Array[Long](nBins)
    val binWidth:Double = (max - min) / nBins

    def add(key: Any): Histogram = {
      if(key != null){
        val value = key match{
          case k:String=> Util.stringToInt(k, nBins)
          case k:Int => k
          case k:Long => k
          case k:Double => k
          case k:java.math.BigDecimal => k.doubleValue()
          case k:java.sql.Date => Util.stringToInt(k.toString, nBins)
          case k:java.sql.Timestamp => Util.stringToInt(k.toString, nBins)
          case _ => {
            println("WARNING " + key.getClass)
            0
          }
        }

        val iBucket= Math.max(Math.min(((value - min) / binWidth).toInt, nBins - 1), 0)
        buckets(iBucket)+=1
      }
      this
    }

    /**
      * Merge two histogram
      * @param other another histogram computed from another partition
      */
    def merge(other: Histogram): this.type = {
      for(i <- 0 until nBins){
        buckets(i) += other.buckets(i)
      }
      this
    }
  }

  /**
    * computing equi-width histograms for all columns of a given dataframe
    * @param df
    * @param mins min values of each columns
    * @param maxs max values of each columns
    * @return
    */
  def execute(df: DataFrame,
              mins:Array[Double],
              maxs: Array[Double], nBins:Array[Int]): DataFrame = {
    val cols = df.columns.toSeq
    val numCols = cols.length

    val histMaps = Seq.tabulate(numCols)(i => new Histogram(mins(i), maxs(i), nBins(i)))
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
    Util.toDataFrame(df.sparkSession, LocalRelation.fromExternalRows(schema, Seq(resultRow)))
  }
}
