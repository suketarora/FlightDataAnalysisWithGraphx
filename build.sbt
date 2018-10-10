name := "FlightDataAnalyticsWithGraphx"

version := "0.1"


scalaVersion := "2.11.8" 

val sparkVersion = "2.3.0"
lazy val spark = "org.apache.spark"



resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  spark %% "spark-core" % sparkVersion,
  spark %% "spark-sql" % sparkVersion,
  spark %% "spark-mllib" % sparkVersion,
  spark %% "spark-graphx" % sparkVersion,
  spark %% "spark-streaming" % sparkVersion,
  spark %% "spark-hive" % sparkVersion
)
