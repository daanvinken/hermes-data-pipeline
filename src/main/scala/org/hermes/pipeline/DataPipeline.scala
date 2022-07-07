package org.hermes.pipeline

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.hermes.pipeline.statistics.models.KSTestResult
import org.hermes.pipeline.workflow.{MeasurementConfig, PreProcessConfig, Source, WorkFlow}

import java.util.Properties

trait DataPipeline{
  def run(): Unit
}


object DataPipeline {
  val LOGGER = Logger.getLogger("DataPipeline")

  def apply(workFlow: WorkFlow, applicationProperties: Properties)(implicit sc: SparkContext): String = {
    val sourceDF = applySource(workFlow.source)

    var parquetLocations = List[String]()

    println(workFlow.preProcessConfig.operations.length)

    if (workFlow.preProcessConfig.operations.nonEmpty) {
      parquetLocations = applyPreProcessing(sourceDF, workFlow.preProcessConfig)
    }
    else {
      parquetLocations = List(workFlow.source.metadata("location_1"), workFlow.source.metadata("location_2"))
    }

    val result = applyStatisticalTest(parquetLocations, workFlow.measurementConfig)

    println("KS Test result: " + result)
    new String("Done")
  }

  private def applySource(source: Source)(implicit SC: SparkContext): DataFrame = {
    val sqlContext = SparkSession.builder().appName("hermes-data-pipeline").master("local").getOrCreate()

    source.sourceType match {
      case "ELASTICSEARCH" => {
        LOGGER.warn("Connecting to Elasticsearch as data source.")
        val sqlContext = SparkSession.builder().appName("hermes-data-pipeline").master("local").getOrCreate()
        val reader = sqlContext.read.
          format("org.elasticsearch.spark.sql").
          option("es.nodes", source.metadata("url")).
          option("es.port", source.metadata("port")).
          option("es.index.auto.create", "true").
          option("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
          option("es.nodes.wan.only", "true")

        val df = reader.load(source.metadata("indexPattern"))

        LOGGER.warn(String.format("Found Elasticsearch schema (%s):\n%s", source.metadata("indexPattern"), df.schema.treeString))
        df
      }
      case "PARQUET" => {
        LOGGER.warn("Using Parquet files as data source.")
      }
      sqlContext.emptyDataFrame
    }

  }

  private def applyValidation(): Unit = {
    null
  }

  private def applyPreProcessing(df: DataFrame, preProcessConfig: PreProcessConfig)(implicit SC: SparkContext): List[String] = {
    /* TODO remove this and create an empty dataframe, but first need sqlcontext */
    var new_df = df

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

  private def applyStatisticalTest(parquetFilePaths: List[String], measurementConfig: MeasurementConfig)(implicit SC: SparkContext): KSTestResult = {
    val sparkSession = SparkSession.builder().appName("hermes-data-pipeline").master("local").getOrCreate()
    println(parquetFilePaths)
    val df1 = sparkSession.read.parquet(parquetFilePaths(0))
    val df2 = sparkSession.read.parquet(parquetFilePaths(1))

    val ksTestResult = TwoSampleKSTest.run_KS(df1, "duration", df2, "duration")
    ksTestResult
  }
}
