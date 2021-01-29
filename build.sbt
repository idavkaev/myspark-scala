name := "myspark-scala"

version := "3.0.1"

scalaVersion := "2.12.2"
val sparkVersion = "3.0.1"
libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.14",
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0" % "test",
  "org.scalatest" %% "scalatest" % sparkVersion % "test",
  "com.typesafe" % "config" % "1.4.1"
)

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
}