package com.waitingforcode.demo3_schema_evolution_step_by_step_no_tracking

import com.waitingforcode.{NumbersWithLettersTable, getOrCreateDeltaLakeSparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener

import java.util.concurrent.atomic.AtomicBoolean

object StreamDeltaTable {

  val StreamStarted = new AtomicBoolean(false)

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateDeltaLakeSparkSession()

    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        StreamStarted.set(true)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    val checkpointDir = s"${DemoDir}/checkpoint"
    sparkSession.readStream.format("delta")
      .table(NumbersWithLettersTable)
      .writeStream
      .option("checkpointLocation", checkpointDir)
      .format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }

}
