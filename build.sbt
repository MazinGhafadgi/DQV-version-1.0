
scalaVersion     := "2.12.13"
version          := "0.1.0-SNAPSHOT"
organization     := "com.vodafone"
organizationName := "vodafone"
name             := "Data_Quality_Tool"
val circeVersion = "0.12.3"


libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"
libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.19.1"
libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "1.113.13"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0"
libraryDependencies += "joda-time" % "joda-time" % "2.10.10"
libraryDependencies += "io.bretty" % "console-table-builder" % "1.2"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyJarName in assembly := "Data-quality-tool-1.0.jar"


mainClass in assembly := Some("vodafone.dataqulaity.app.DataQualityCheckApp")
