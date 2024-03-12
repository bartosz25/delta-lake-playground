package com.waitingforcode.demo1_idempotent_fan_out

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake
import org.apache.commons.io.FileUtils

import java.io.File

object NotIdempotentFanOutProducerFailedRun {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(DemoDir))
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    val streamSource = getRateStreamSource(sparkSession)

    streamSource.writeStream.option("checkpointLocation", CheckpointLocation)
      .format("delta")
      .foreachBatch(IdempotentForeachBatchWriter.getWriter(shouldBeIdempotent = false, shouldFail = true))
      .start()

    sparkSession.streams.awaitAnyTermination(45000L)
  }

}
