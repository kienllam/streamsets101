name := "streamsets101"

version := "0.1"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "com.streamsets" % "streamsets-datacollector-spark-api" % "2.7.0.0",
  "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.12.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.0-cdh5.12.1"
)