package org.hermes.pipeline

import org.hermes.pipeline.workflow.{MeasurementConfig, PreProcessConfig, Source, WorkFlow}
import org.hermes.pipeline.spark.SparkContextProvider
import org.hermes.pipeline.models.TraceRecord
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Properties;

trait DataPipeline{
  def run(): Unit
}


object DataPipeline {
  val LOGGER = Logger.getLogger("DataPipeline")

  def apply(workFlow: WorkFlow, applicationProperties: Properties)(implicit sc: SparkContext): String = {
    val emptyMap = Map.empty[String, String]
    val source = workFlow.source
    val sourceDF = applySource(source)
    val preProcessedDF = applyPreProcessing(sourceDF, workFlow.preProcessConfig)

    new String(preProcessedDF.summary().toString())
  }

  private def applySource(source: Source)(implicit sc: SparkContext): DataFrame = {

    source.sourceType match {
      case "ES" =>
        LOGGER.warn("Connecting to Elasticsearch...")
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val reader = sqlContext.read.
          format("org.elasticsearch.spark.sql").
          option("es.nodes", source.url).
          option("es.port", source.port.toString()).
          option("es.index.auto.create", "true").
          option("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
          option("es.nodes.wan.only", "true")

        val df = reader.load(source.indexPattern)

        LOGGER.warn(String.format("Found Elasticsearch schema (%s):\n%s", source.indexPattern, df.schema.treeString))
        df
    }

  }

  private def applyPreProcessing(df: DataFrame, preProcessConfig: PreProcessConfig)(implicit sc: SparkContext): DataFrame = {
    /* Drop columns */
    val toDrop = preProcessConfig.columnsToDrop
    val new_df = df.drop(toDrop : _*)

    /* Split into parquet files per operation */
    new_df.printSchema()
    new_df.write.partitionBy("operationName").saveAsTable(tableName="splittedOperations")

    new_df.describe().show()
    new_df.printSchema()
    new_df
  }

  private def applyMeasurements(df: DataFrame, measurementConfig: MeasurementConfig)(implicit sc: SparkContext): DataFrame = {
    df
  }
}
