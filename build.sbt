name := "Gapp"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "1.2.0",
  "org.apache.spark" %% "spark-graphx" % "latest.integration"
)
