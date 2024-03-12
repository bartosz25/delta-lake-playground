package com.waitingforcode.demo2_new_columns

import com.waitingforcode.{DataWarehouseBaseDir, NumbersWithLettersTable, getOrCreateSparkSessionWithDeltaLake}
import org.apache.spark.sql.streaming.StreamingQueryListener

import java.util.concurrent.atomic.AtomicBoolean

object StreamingReader {

  val StreamStarted = new AtomicBoolean(false)
  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()

    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        StreamStarted.set(true)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    val checkpointDir = s"${DemoDir}/checkpoint"
    sparkSession.readStream.format("delta").load(s"${DataWarehouseBaseDir}/${NumbersWithLettersTable}")
      .writeStream
      .option("checkpointLocation", checkpointDir)
      .format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }

}
