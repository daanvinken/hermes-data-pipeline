ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "hermes-data-pipeline",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8",
    // https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
    libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.17.2",

    // https://mvnrepository.com/artifact/org.antlr/antlr4-runtime
    libraryDependencies += "org.antlr" % "antlr4-runtime" % "4.9.1",
    // https://mvnrepository.com/artifact/org.apache.parquet/parquet-hadoop
    libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.12.0",


    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36",
    // https://mvnrepository.com/artifact/org.json4s/json4s-native
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.4",
    // https://mvnrepository.com/artifact/org.scala-lang.modules/scala-xml
    libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.3.0"














  )
