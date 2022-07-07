package org.hermes.pipeline.workflow

case class WorkFlow(
                     source: Source,
                     preProcessConfig: PreProcessConfig,
                     measurementConfig: MeasurementConfig
                   )

case class Source(sourceType: String,
                  metadata: Map[String, String])

case class PreProcessConfig(
                            operations: List[String],
                            outputName: String,
                            metadata: Map[String, Object]
                           )

case class MeasurementConfig(statisticalTestType: String)
