package org.hermes.pipeline

import org.hermes.pipeline.DataPipeline;
import org.hermes.pipeline.workflow.WorkFlow;
import org.hermes.pipeline.workflow.Source;
import org.hermes.pipeline.spark.SparkContextProvider;

object RunHermesDataPipeline extends App with SparkContextProvider{

  val workFlow = new WorkFlow("Daan");

  val result = DataPipeline.apply(workFlow)
  println(result)

  sc.stop()

}
