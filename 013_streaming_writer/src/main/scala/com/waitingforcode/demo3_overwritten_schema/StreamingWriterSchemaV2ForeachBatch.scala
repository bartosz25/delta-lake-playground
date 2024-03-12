package com.waitingforcode.demo3_overwritten_schema

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake
import org.apache.spark.sql.{DataFrame, SaveMode, functions}

object StreamingWriterSchemaV2ForeachBatch {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(3)

    val rateStreamInput = getRateStreamSource(sparkSession)

    val rateStreamInputWithNewColumn = rateStreamInput.withColumn("value",
      functions.array(functions.lit("a"), functions.lit("b")))

    rateStreamInputWithNewColumn.writeStream
      .option("checkpointLocation", CheckpointLocation)
      .foreachBatch((dataFrame: DataFrame, version: Long) => {
        dataFrame.write
          .mode(SaveMode.Overwrite)
          .option("overwriteSchema", true)
          .format("delta")
          //.insertInto(TableName)
          .saveAsTable(TableName)
        ()
      }).start()

    sparkSession.streams.awaitAnyTermination(30_000L)
  }
}
