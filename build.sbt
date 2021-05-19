name := "MyStreams"

enablePlugins(JavaAppPackaging)

version := "0.1"

scalaVersion := "2.12.6"

val AkkaVersion = "2.6.10"
val kafkaVersion = "2.6.0"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.5",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)
libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
