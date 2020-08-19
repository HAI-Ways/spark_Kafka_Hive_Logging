name := "spark_Kafka_Hive_Logging"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.2" % Test
)
