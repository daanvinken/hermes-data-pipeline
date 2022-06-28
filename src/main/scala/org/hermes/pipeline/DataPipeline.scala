package org.hermes.pipeline

import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.hermes.pipeline.workflow.{MeasurementConfig, PreProcessConfig, Source, WorkFlow}
import org.hermes.pipeline.statistics.TwoSampleKSTest
import org.hermes.pipeline.statistics.models.KSTestResult
import org.hermes.pipeline.util.Utils.generateRandomUniformDf

import java.util.Properties;

trait DataPipeline{
  def run(): Unit
}


object DataPipeline {
  val LOGGER = Logger.getLogger("DataPipeline")

  def apply(workFlow: WorkFlow, applicationProperties: Properties)(implicit sc: SparkContext): String = {
    val emptyMap = Map.empty[String, String]
    val source = workFlow.source
    val KsTestTest = runKsTest()
    val sourceDF = applySource(source)
    val parquetLocations = applyPreProcessing(sourceDF, workFlow.preProcessConfig)
//    val parquetLocations =  List[String]("/Users/daanvi/workspace/hermes-data-pipeline/spark-warehouse/splittedoperations_0/operationName=%2Forders%2Fcreate%2Fid%2F%7Bid}")
    applyMeasurements(parquetLocations, workFlow.measurementConfig)

    new String("Done")
  }

  private def runKsTest()(implicit SC: SparkContext): KSTestResult = {
    val df1 = generateRandomUniformDf(1000000, 1, SC)
    val df2 = generateRandomUniformDf(1000000, 1, SC)
    val ksTestResult = TwoSampleKSTest.run_KS(df1, "val0", df2, "val0")
    ksTestResult
  }

  private def applySource(source: Source)(implicit SC: SparkContext): DataFrame = {

    source.sourceType match {
      case "ES" =>
        LOGGER.warn("Connecting to Elasticsearch...")
        val sqlContext = SparkSession.builder().appName("hermes-data-pipeline").master("local").getOrCreate()
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

  private def applyPreProcessing(df: DataFrame, preProcessConfig: PreProcessConfig)(implicit SC: SparkContext): List[String] = {
    /* Drop columns */
    val toDrop = preProcessConfig.columnsToDrop
    val new_df = df.drop(toDrop : _*)

    /* Split into parquet files per operation */
    new_df.printSchema()
    new_df.write.partitionBy("operationName").saveAsTable(tableName="splittedOperations")

    new_df.describe().show()
    new_df.printSchema()
    val parquetLocations =  List[String]("/Users/daanvi/workspace/hermes-data-pipeline/spark-warehouse/splittedoperations_0/operationName=%2Forders%2Fcreate%2Fid%2F%7Bid}")
    parquetLocations
  }

  private def applyMeasurements(parquetFilePaths: List[String], measurementConfig: MeasurementConfig)(implicit SC: SparkContext): Boolean = {
    val sparkSession = SparkSession.builder().appName("hermes-data-pipeline").master("local").getOrCreate()
    for (parquetFilePath <- parquetFilePaths) {
      val parqDF = sparkSession.read.parquet(parquetFilePath)
      parqDF.printSchema()
    }
    /* TODO apply KS-test as measurement
    Perhaps we'd like this in a different class to have some kind of scheduler
     */


    true
  }
}
