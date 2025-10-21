package com.waitingforcode.demo4_decorators_and_streaming_writer

import com.waitingforcode._

import scala.util.Random

object Demo4 {

  def main(args: Array[String]): Unit = {
    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 1)
      .option("numPartitions", 1)
      .load()

    def getRandomNumber() = {
      Random.nextInt(50)
    }

    val query = rateStreamInput.writeStream
      .option("checkpointLocation", s"/tmp/checkpoint${System.currentTimeMillis()}")
      .format("delta")
      // userMetadata is also supported for streaming writer; however, the metadata here is resolved
      // only once, so you won't be able to do anything dynamic
      .option("userMetadata", s"writer=streaming,version=v1,random=${getRandomNumber()}")
      .toTable("demo4")

    query.awaitTermination(50000L)


    sparkSession.sql("DESCRIBE HISTORY demo4").show(truncate=false)
    val userMetadata = sparkSession.sql("DESCRIBE HISTORY demo4").select("userMetadata")
    userMetadata.printSchema()
    userMetadata.show(false)
  }
}
