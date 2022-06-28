package org.hermes.pipeline

import org.hermes.pipeline.util.JSONHelper;
import org.hermes.pipeline.workflow.WorkFlow;
import org.hermes.pipeline.workflow.Source;
import org.hermes.pipeline.sparkcontext.SparkContextProvider;
import java.util.Properties;
import java.nio.file.Files
import java.nio.file.Path
import java.io.FileReader

object RunHermesDataPipeline extends App with SparkContextProvider with JSONHelper{
   if (args.length < 1)
        throw new IllegalArgumentException("Path to workflow json is required")

  val workFlowLocation = Path.of(args(0))
  val workFlowJson = Files.readString(workFlowLocation)
  val workFlow = parse(workFlowJson).extract[WorkFlow]

  val propertiesReader = new FileReader("resources/application.properties")
  val applicationProperties = new Properties()
  applicationProperties.load(propertiesReader)

  val result = DataPipeline.apply(workFlow, applicationProperties)

  println(result)

  sc.stop()

}
