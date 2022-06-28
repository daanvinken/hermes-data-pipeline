package org.hermes.pipeline.util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

object Utils {
  def generateRandomUniformDf(numRows: Integer, numCols: Integer, SC: SparkContext): DataFrame = {
    val vectorRDD = RandomRDDs.uniformVectorRDD(SC, numRows.toLong, numCols) // Calculate the maximum length of the vector to create a schema
    val vectorLength = vectorRDD.map(x => x.toArray.length).max()

    // create the dynamic schema
    var schema = new StructType()
    var i = 0
    while (i < vectorLength) {
      schema = schema.add(StructField(s"val${i}", DoubleType, nullable=true))
      i = i + 1
    }

    // create a rowRDD variable and make each row have the same arity
    val rowRDD = vectorRDD.map { x =>
      var row = new Array[Double](vectorLength)
      val newRow = x.toArray

      System.arraycopy(newRow, 0, row, 0, newRow.length);

      Row.fromSeq(row)
    }

    // create your dataframe
    val sqlContext = SparkSession.builder().appName("hermes-data-pipeline").master("local").getOrCreate()
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.describe().show()
    df
  }
}
