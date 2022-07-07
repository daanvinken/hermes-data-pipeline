package org.hermes.pipeline

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.hermes.pipeline.statistics.TwoSampleKSTest
import org.hermes.pipeline.statistics.models.KSTestResult
import org.hermes.pipeline.util.Utils.getElasticReader
import org.hermes.pipeline.workflow.{MeasurementConfig, PreProcessConfig, Source, WorkFlow}

import java.util.Properties

trait DataPipeline{
  def run(): Unit
}


object DataPipeline {
  val LOGGER = Logger.getLogger("DataPipeline")

  def apply(workFlow: WorkFlow, applicationProperties: Properties)(implicit sc: SparkContext, sq: SparkSession): Unit = {
    val sourceDF = applySource(workFlow.source)

    var parquetLocations = List[String]()

    if (workFlow.preProcessConfig.operations.nonEmpty) {
      parquetLocations = applyPreProcessing(sourceDF, workFlow.preProcessConfig)
    }
    else {
      parquetLocations = List(workFlow.source.metadata("location_1"), workFlow.source.metadata("location_2"))
    }

    val result = applyStatisticalTest(parquetLocations, workFlow.measurementConfig)

    LOGGER.info("KS Test result:\n" + "\t\t\t\t\t| Distance D = " + result.distance + "\n\t\t\t\t\t" + "| p-value = " + result.pValue)

  }

  private def applySource(source: Source)(implicit SC: SparkContext, SQ: SparkSession): DataFrame = {

    source.sourceType match {
      case "ELASTICSEARCH" => {
        LOGGER.info("Connecting to Elasticsearch as data source.")
        val reader = getElasticReader(source)
        val df = reader.load(source.metadata("indexPattern"))
        LOGGER.info(String.format("Found Elasticsearch schema (%s):\n%s", source.metadata("indexPattern"), df.schema.treeString))
        df
      }
      case "PARQUET" => {
        LOGGER.info("Using Parquet files as data source.")
        SQ.emptyDataFrame
      }
    }

  }

  private def applyValidation(): Unit = {
  }

  private def applyPreProcessing(df: DataFrame, preProcessConfig: PreProcessConfig)(implicit SC: SparkContext, SQ: SparkSession): List[String] = {
    var new_df = SQ.emptyDataFrame

    preProcessConfig.operations.foreach {
      case "DROP_COLUMNS" => {
        val toDrop = preProcessConfig.metadata("columnsToDrop").asInstanceOf[List[String]]
        val new_df = df.drop(toDrop: _*)
      }
      case default => throw new IllegalArgumentException("'$default' is not a known operation for preprocessing.")
    }

    /* Split into parquet files per operation */
    new_df.printSchema()
    new_df.write.partitionBy("operationName").saveAsTable(tableName=preProcessConfig.outputName)

    new_df.describe().show()
    new_df.printSchema()
    val parquetLocations =  List[String]("/Users/daanvi/workspace/hermes-data-pipeline/spark-warehouse/" + preProcessConfig.outputName)
    parquetLocations
  }

  private def applyStatisticalTest(parquetFilePaths: List[String], measurementConfig: MeasurementConfig)(implicit SC: SparkContext, SQ: SparkSession): KSTestResult = {
    val df1 = SQ.read.parquet(parquetFilePaths(0))
    val df2 = SQ.read.parquet(parquetFilePaths(1))
    LOGGER.info("Starting KS-test")
    val ksTestResult = TwoSampleKSTest.run_KS(df1, "duration", df2, "duration")
    LOGGER.info("Done")
    ksTestResult
  }
}
