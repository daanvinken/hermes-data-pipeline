package org.hermes.pipeline.statistics

import org.hermes.pipeline.statistics.models.KSTestResult
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

import scala.util.Try


/* Based on https://github.com/Davi-Schumacher/KS-2Samp-PySparkSQL/blob/master/ks_2samp_sparksql.py
*  And verified with  Nonparametric Statistical Methods - O'Reilly
* A1  - Hollander, M.
  A1  - Wolfe, D.A.
  A1  - Chicken, E.
  SN  - 9781118553299
  T3  - Wiley Series in Probability and Statistics
  UR  - https://books.google.nl/books?id=Y5s3AgAAQBAJ
  Y1  - 2013
  PB  - Wiley
* */

object TwoSampleKSTest{
  /* Implementation of the two-sided two-sample Kolomgorov-Smirnov Test */

  val CDF_1 = "cdf_1"
  val CDF_2 = "cdf_2"
  val FILLED_CDF_1 = "filled_cdf_1"
  val FILLED_CDF_2 = "filled_cdf_2"

  def AnyToDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }

  def getCDF(df: DataFrame, variable: String, columnName: String): DataFrame = {
    val cdf = df.select(variable).na.drop()
                .withColumn(columnName, functions.cume_dist().over(Window.orderBy(variable)))
    cdf
  }

  def calculatePValue(KsStatistic: Double, df_1: DataFrame, variable_1: String, df_2: DataFrame, variable_2: String): Double = {
    /* Estimation P-value coming from book Nonparametric Statistical Methods - O'Reilly */
    val n = df_1.select(variable_1).na.drop().count()
    val m = df_2.select(variable_1).na.drop().count()
    val Z = KsStatistic * scala.math.sqrt(m * n / (m + n))

    if (Z < 0) {
      throw new RuntimeException("Z value (coming from maximum distance (KsStatistic)) cannot by < 0.")
    }

    if (0 <= Z && Z < 0.27) {
      val p = 1
      p
    }
    else if (0.27 <= Z && Z < 1) {
      val Q = math.exp(-1.233701 * scala.math.pow(Z, -2))
      val p = 1 - ((2.506628) / Z) * (Q + math.pow(Q, 9) + math.pow(Q, 25))
      p
    }
    else if (1 <= Z && Z < 3.1) {
      val Q = math.exp(-2 * scala.math.pow(Z, 2))
      val p = 2 * (Q - math.pow(Q, 4) + math.pow(Q, 9) - math.pow(Q, 16))
      p
    }
    else { // Z >= 3.1
      val p = 0
      p
    }
  }

  def run_KS(df_1: DataFrame, variable_1: String, df_2: DataFrame, variable_2: String): KSTestResult = {
    val cdf_1 = getCDF(df_1, variable_1, CDF_1).as("cdf_1")
    val cdf_2 = getCDF(df_2, variable_2, CDF_2).as("cdf_2")
    val cdfs = cdf_1.join(cdf_2, col(CDF_1 + "." + variable_1) === col(CDF_2 + "." + variable_2), "outer")

    val KsStatisticResult = cdfs
      .withColumn(FILLED_CDF_1,
        functions.last(col(CDF_1), ignoreNulls = true)
          .over(Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .withColumn(FILLED_CDF_2,
        functions.last(col(CDF_2), ignoreNulls = true)
          .over(Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .select(functions.max(col(FILLED_CDF_1) - col(FILLED_CDF_2)))
      .collect()(0)(0)

    val KsStatistic : Double = AnyToDouble(KsStatisticResult)

    val pValue = calculatePValue(KsStatistic, df_1, variable_1, df_2, variable_2)

    KSTestResult(KsStatistic, pValue)
  }

}
