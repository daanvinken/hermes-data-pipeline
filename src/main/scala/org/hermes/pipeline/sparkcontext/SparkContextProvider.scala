package org.hermes.pipeline.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}


trait SparkContextProvider {

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Hermes")
  implicit val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("WARN")
}
