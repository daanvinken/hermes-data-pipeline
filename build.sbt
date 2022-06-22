ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "hermes-data-pipeline",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8",
    // https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
    libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.17.2"   
)
