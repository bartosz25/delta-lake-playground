package com.waitingforcode.demo8_streaming_reader_with_cdf_table_with_updates_and_deletes

import com.waitingforcode.{DataWarehouseBaseDir, NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.spark.sql.streaming.StreamingQueryListener

import java.util.concurrent.atomic.AtomicBoolean

object StreamingReader {

  val StreamStarted = new AtomicBoolean(false)

  private val DemoDir = s"${OutputDir}/demo7-${System.currentTimeMillis()}"
  //private val DemoDir = s"${OutputDir}/demo7-static"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(extraConfig = Map(
    // Let's set it as default for all created tables
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed" -> "true"
  ))
    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        StreamStarted.set(true)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })


    sparkSession.readStream.format("delta")
      .option("startingVersion", 0)
      .option("skipChangeCommits", true)
      .option("readChangeFeed", true)
      .load(s"${DataWarehouseBaseDir}/${NumbersWithLettersTable}")
      .writeStream
      .option("checkpointLocation", s"${DemoDir}/checkpoint")
      .format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }
}