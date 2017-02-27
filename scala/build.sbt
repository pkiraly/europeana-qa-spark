name := "Europeana QA"

version := "1.0"

scalaVersion := "2.10.5"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.commons" % "commons-math3" % "3.6.1"
)
