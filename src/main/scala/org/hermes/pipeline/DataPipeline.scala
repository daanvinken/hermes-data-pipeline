package org.hermes.pipeline

import org.hermes.pipeline.workflow.WorkFlow
import org.hermes.pipeline.spark.SparkContextProvider
import org.hermes.pipeline.models.TraceRecord
import org.hermes.pipeline.workflow.Source
import org.hermes.pipeline.workflow.PreProcessConfig
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
    val preProcessedRDD = applyPreProcessing(sourceDF, workFlow.preProcessConfig)

    new String("success")
  }
// /vagrant/src/main/resources/workflowDefiniton.json

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

        var df = reader.load(source.indexPattern)

        LOGGER.warn(String.format("Found Elasticsearch schema (%s):\n%s", source.indexPattern, df.schema.treeString))
        df.describe().show()
        return df
    }

  }

  private def applyPreProcessing(df: DataFrame, preProcessConfig: PreProcessConfig)(implicit sc: SparkContext): DataFrame = {
    val toDrop = preProcessConfig.columnsToDrop
    val new_df = df.drop(toDrop : _*)
    new_df.describe().show()
    LOGGER.info("Test")
    new_df.printSchema()
    return new_df
  }
}
