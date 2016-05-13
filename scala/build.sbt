name := "Europeana QA"

version := "1.0"

scalaVersion := "2.10.5"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
)
