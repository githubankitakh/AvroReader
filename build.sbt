name := "PackageAdvancedShipmentNotice"

version := "0.1"

scalaVersion := "2.12.10"

val confluentVersion = "5.4.1"

val log4jVersion = "2.4.1"

val sparkVersion = "3.0.0"

val kafkaVersion = "2.4.0"


resolvers ++= Seq(
  "confluent" at "https://packages.confluent.io/maven/",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)






libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,


  "org.apache.spark" %% "spark-avro" % sparkVersion,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  "io.confluent" % "kafka-schema-registry-client" % confluentVersion,
  "io.confluent" % "kafka-avro-serializer" % confluentVersion,

  "net.snowflake" %% "spark-snowflake" % "2.8.2-spark_3.0",

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion

)
