name := "kafka"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.kafka" % "kafka-clients" % "2.1.1"
)