package org.hermes.pipeline.providers

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


trait SparkSessionProvider {
  implicit val sq: SparkSession= SparkSession.builder().appName("hermes-data-pipeline").master("local").getOrCreate()
}
