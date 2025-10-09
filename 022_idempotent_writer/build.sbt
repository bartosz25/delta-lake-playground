scalaVersion := "2.13.16"
name := "delta_lake_idempotent_writer"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "4.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "io.delta" %% "delta-spark" % "4.0.0"

