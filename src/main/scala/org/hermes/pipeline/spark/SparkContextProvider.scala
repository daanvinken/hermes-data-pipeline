package org.hermes.pipeline.spark

import org.apache.spark.{SparkConf, SparkContext}


trait SparkContextProvider {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Hermes")
  implicit val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
}
