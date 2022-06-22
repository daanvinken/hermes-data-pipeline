package org.hermes.pipeline

import org.hermes.pipeline.workflow.WorkFlow
import org.hermes.pipeline.spark.SparkContextProvider
import org.hermes.pipeline.models.TraceRecord
import org.hermes.pipeline.workflow.Source
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait DataPipeline {

  def run(): Unit
}


object DataPipeline {
  def apply(workFlow: WorkFlow)(implicit sc: SparkContext): String = {
    val emptyMap = Map.empty[String, String]
    val source = new Source("xx", "yy", emptyMap)
    val sourceRDD = applySource(source)
    new String("success")
  }


  private def applySource(source: Source)(implicit sc: SparkContext): RDD[TraceRecord] = {
    println("Connecting to Elasticsearch.")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val reader = sqlContext.read.
      format("org.elasticsearch.spark.sql").
      option("es.nodes","app1.daan-vinken.sandbox01.adyen.com").
      option("es.port","9200")
    var df = reader.load("jaeger-span*")
    df.printSchema()
    sc.emptyRDD[TraceRecord]
  }
}
