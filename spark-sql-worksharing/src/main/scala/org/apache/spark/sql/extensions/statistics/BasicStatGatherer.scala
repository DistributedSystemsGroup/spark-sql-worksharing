package org.apache.spark.sql.extensions.statistics

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.extensions.Util

/**
  * Obtaining statistics information from built-in SQL DataFrame
  * Spark SQL currently supports basic statistics for numeric columns, see the ``describe" function
  * Ref: https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
  * We can obtain: [(column_name, (count, mean, stddev, min, max))]
  * Customized version:
  * add ApproxCountDistinct for each column (cardinality estimation with the HyperLogLog algorithm)
  */
object BasicStatGatherer {

  /**
    * compute basic statistics for a given dataframe:
    * [count (not null), min, max, ApprCountDistinct]
    *
    * @param df
    * @return
    *
    */
  def execute(df: DataFrame): DataFrame = {
    //TODO: How about using dataframe.stat.corr('column1', 'column2')? to compute the statistical dependence between 2 columns?

    // The list of summary statistics to compute, in the form of expressions.
    val statistics = List[(String, Expression => Expression)](
      "count" -> ((child: Expression) => Count(child).toAggregateExpression()), // count not null
      "min" -> ((child: Expression) => Min(child).toAggregateExpression()),
      "max" -> ((child: Expression) => Max(child).toAggregateExpression()),
      "ApprCountDistinct" -> ((child: Expression) => HyperLogLogPlusPlus(child).toAggregateExpression()))

    val outputCols = df.columns.toList

    val ret: Seq[Row] = if (outputCols.nonEmpty) {
      val aggExprs = statistics.flatMap { case (_, colToAgg) =>
        outputCols.map(c => Column(Cast(colToAgg(Column(c).expr), StringType)).as(c))
      }

      val row = df.groupBy().agg(aggExprs.head, aggExprs.tail: _*).head().toSeq

      // Pivot the data so each summary is one row
      row.grouped(outputCols.size).toSeq.zip(statistics).map { case (aggregation, (statistic, _)) =>
        Row(statistic :: aggregation.toList: _*)
      }
    } else {
      // If there are no output columns, just output a single column that contains the stats.
      statistics.map { case (name, _) => Row(name) }
    }

    val schema = StructType(StructField("summary", StringType) :: outputCols.map(StructField(_, StringType))).toAttributes
    Util.toDataFrame(df.sqlContext.sparkSession, LocalRelation.fromExternalRows(schema, ret))
  }
}
