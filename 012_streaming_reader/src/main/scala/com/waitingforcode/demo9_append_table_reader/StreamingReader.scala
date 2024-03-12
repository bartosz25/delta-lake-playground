package com.waitingforcode.demo9_append_table_reader

import com.waitingforcode.{DataWarehouseBaseDir, NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.spark.sql.streaming.StreamingQueryListener

import java.util.concurrent.atomic.AtomicBoolean

object StreamingReader {

  val StreamStarted = new AtomicBoolean(false)

  private val DemoDir = s"${OutputDir}/demo6-${System.currentTimeMillis()}"
  //private val DemoDir = s"${OutputDir}/demo6-static"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        StreamStarted.set(true)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    sparkSession.readStream.format("delta")
      //.option("startingVersion", 0)
      //.option("skipChangeCommits", false) //.option("skipChangeCommits", true)
      .load(s"${DataWarehouseBaseDir}/${NumbersWithLettersTable}")
      .writeStream
      .option("checkpointLocation", s"${DemoDir}/checkpoint")
      .format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }
}