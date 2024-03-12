package com.waitingforcode.demo1_idempotent_fan_out

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake

object IdempotentFanOutProducerSucceededRun {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    val streamSource = getRateStreamSource(sparkSession)

    streamSource.writeStream.option("checkpointLocation", CheckpointLocation)
      .format("delta")
      .foreachBatch(IdempotentForeachBatchWriter.getWriter(shouldBeIdempotent = true, shouldFail = false))
      .start()

    sparkSession.streams.awaitAnyTermination(45000L)

  }

}
