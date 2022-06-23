package org.hermes.pipeline.workflow

case class WorkFlow(
                     source: Source,
                     preProcessConfig: PreProcessConfig
                   )

case class Source(sourceType: String, url: String, port: Integer, indexPattern: String, metadata: Map[String, String])

case class PreProcessConfig(columnsToDrop: List[String])
