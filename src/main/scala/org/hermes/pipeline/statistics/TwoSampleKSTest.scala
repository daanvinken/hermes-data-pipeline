package org.hermes.pipeline.statistics

import org.hermes.pipeline.statistics.models.KSTestResult
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col


/* Ported from https://github.com/Davi-Schumacher/KS-2Samp-PySparkSQL/blob/master/ks_2samp_sparksql.py */

object TwoSampleKSTest extends App{
  /* Implementation of the two-sided two-sample Kolomgorov-Smirnov Test */

  val CDF_1 = "cdf_1"
  val CDF_2 = "cdf_2"
  val FILLED_CDF_1 = "filled_cdf_1"
  val FILLED_CDF_2 = "filled_cdf_2"

  def getCDF(df: DataFrame, variable: String, columnName: String): DataFrame = {
    val cdf = df.select(variable).na.drop()
                .withColumn(columnName, functions.cume_dist().over(Window.orderBy(variable)))
    cdf
  }

  def run_KS(df_1: DataFrame, variable_1: String, df_2: DataFrame, variable_2: String): KSTestResult = {
    val cdf_1 = getCDF(df_1, variable_1, CDF_1).as("cdf_1")
    val cdf_2 = getCDF(df_2, variable_2, CDF_2).as("cdf_2")
    val cdfs = cdf_1.join(cdf_2, col(variable_1) === col(variable_2), "outer")

    val KsStatistic = cdfs
      .withColumn(FILLED_CDF_1,
        functions.last(col(CDF_1), ignoreNulls = true)
        .over(Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .withColumn(FILLED_CDF_2,
        functions.last(col(CDF_2), ignoreNulls = true)
        .over(Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .select(functions.max(col(FILLED_CDF_1) - col(FILLED_CDF_2)))
      .collect()
    println(KsStatistic.mkString("Array(", ", ", ")"))

  val n1 = df_1.select(variable_1).na.drop().count()
  val n2 = df_2.select(variable_2).na.drop().count()
  val en = scala.math.sqrt(n1 * n2 / (n1 + n2).toFloat)


    println("Test")
    KSTestResult(0.5, 5)
  }
  

}
