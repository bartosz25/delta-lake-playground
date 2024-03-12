package com.waitingforcode.demo3_overwritten_schema

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake
import org.apache.spark.sql.functions

object StreamingWriterSchemaV2StreamingSink {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(3)

    val rateStreamInput = getRateStreamSource(sparkSession)

    val rateStreamInputWithNewColumn = rateStreamInput.withColumn("value",
      functions.array(functions.lit("a"), functions.lit("b")))

    rateStreamInputWithNewColumn.writeStream
      .option("checkpointLocation", CheckpointLocation)
      .option("overwriteSchema", true)
      .format("delta")
      .toTable(TableName)

    sparkSession.streams.awaitAnyTermination(30_000L)
  }
}
