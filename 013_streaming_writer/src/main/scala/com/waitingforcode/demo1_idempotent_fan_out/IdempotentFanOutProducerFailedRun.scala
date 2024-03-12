package com.waitingforcode.demo1_idempotent_fan_out

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake
import org.apache.commons.io.FileUtils

import java.io.File

object IdempotentFanOutProducerFailedRun {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(DemoDir))
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    val streamSource = getRateStreamSource(sparkSession)

    streamSource.writeStream.option("checkpointLocation", CheckpointLocation)
      .format("delta")
      .foreachBatch(IdempotentForeachBatchWriter.getWriter(shouldBeIdempotent = true, shouldFail = true))
      .start()

    sparkSession.streams.awaitAnyTermination(45000L)
  }

}
