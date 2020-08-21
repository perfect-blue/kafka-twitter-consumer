name := "kafka-twitter-consumer"

version := "0.1"

scalaVersion := "2.11.12"

val kafkaVersion= "2.4.0"
val sparkVersion = "2.4.5"
val log4jVersion = "2.4.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  //spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  //streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  //streaming-kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion ,

  //low-level integration
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  //kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion


)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}