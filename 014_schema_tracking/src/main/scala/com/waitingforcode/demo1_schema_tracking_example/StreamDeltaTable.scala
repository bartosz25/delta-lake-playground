package com.waitingforcode.demo1_schema_tracking_example

import com.waitingforcode.{NumbersWithLettersTable, getOrCreateDeltaLakeSparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener

import java.util.concurrent.atomic.AtomicBoolean

object StreamDeltaTable {

  val StreamStarted = new AtomicBoolean(false)

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateDeltaLakeSparkSession(extraConfig = Map(
      "spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop" -> "always"
    ))

    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        StreamStarted.set(true)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    val checkpointDir = s"${DemoDir}/checkpoint"
    sparkSession.readStream.format("delta")
      .option("schemaTrackingLocation", s"${checkpointDir}/schema_tracking")
      .table(NumbersWithLettersTable)
      .writeStream
      .option("checkpointLocation", checkpointDir)
      .format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }

}
