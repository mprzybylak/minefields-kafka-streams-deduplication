name := "minefields-kafka-streams-deduplication"

version := "0.1"

scalaVersion := "2.12.6"

val scalaTestVersion = "3.0.4"
val mockito = "2.10.0"
val kafkaVersion = "1.0.0"
val embeddedKafkaVersion = "1.0.0"

val dependencies = Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.mockito" % "mockito-core" % mockito % Test,
  "net.manub" %% "scalatest-embedded-kafka-streams" % embeddedKafkaVersion % Test,
  "net.manub" %% "scalatest-embedded-kafka" % embeddedKafkaVersion % Test
)

libraryDependencies ++= dependencies ++ testDependencies