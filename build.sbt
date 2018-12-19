name := "timeseries"
scalaVersion        := "2.11.11"

lazy val buildSettings = Seq(
  organization        := "gaion",
  version             := "0.1.0"
)

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.1"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
