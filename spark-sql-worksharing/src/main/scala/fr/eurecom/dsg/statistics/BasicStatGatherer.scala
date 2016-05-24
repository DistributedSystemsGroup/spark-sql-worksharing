package org.apache.spark.sql.myExtensions.statistics

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

/**
  * Obtaining statistics information from built-in SQL DataFrame
  * Spark SQL currently supports basic statistics for numeric columns
  * Ref: https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
  * We can obtain: [(column_name, (count, mean, stddev, min, max))] from it
  * Customized version:
  * add ApproxCountDistinct for each column (cardinality estimation with the HyperLogLog algorithm)
  */
object BasicStatGatherer {

  def execute(sqlc:SQLContext, df:DataFrame):DataFrame={
    def stddevExpr(expr: Expression): Expression =
      Sqrt(Subtract(Average(Multiply(expr, expr)), Multiply(Average(expr), Average(expr))))

    // The list of summary statistics to compute, in the form of expressions.
    val statistics = List[(String, Expression => Expression)](
      "count" -> ((child: Expression) => Count(child).toAggregateExpression()), // count not null
      //"mean" -> Average,
      //"stddev" -> stddevExpr,
      "min" -> ((child: Expression) => Min(child).toAggregateExpression()),
      "max" -> ((child: Expression) => Max(child).toAggregateExpression()),
      "ApprCountDistinct" -> ((child: Expression) => HyperLogLogPlusPlus(child).toAggregateExpression()))

    val outputCols = df.columns.toList

    val ret: Seq[Row] = if (outputCols.nonEmpty) {
      val aggExprs = statistics.flatMap { case (_, colToAgg) =>
        outputCols.map(c => Column(Cast(colToAgg(Column(c).expr), StringType)).as(c))
      }

      val row = df.agg(aggExprs.head, aggExprs.tail: _*).head().toSeq

      // Pivot the data so each summary is one row
      row.grouped(outputCols.size).toSeq.zip(statistics).map { case (aggregation, (statistic, _)) =>
        Row(statistic :: aggregation.toList: _*)
      }
    } else {
      // If there are no output columns, just output a single column that contains the stats.
      statistics.map { case (name, _) => Row(name) }
    }

    val schema = StructType(
      StructField("summary", StringType) :: outputCols.map(StructField(_, StringType))).toAttributes
    Dataset.ofRows(sqlc.sparkSession, LocalRelation.fromExternalRows(schema, ret))
  }
}


