package com.waitingforcode.demo2_changed_schema_with_automerge

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake
import org.apache.spark.sql.functions

object StreamingWriterSchemaV2 {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(2)

    val rateStreamInput = getRateStreamSource(sparkSession)

    val rateStreamInputWithNewColumn = rateStreamInput.withColumn("a", functions.lit("a"))

    rateStreamInputWithNewColumn.writeStream.option("checkpointLocation", CheckpointLocation)
       .option("mergeSchema", true)
      .format("delta").toTable(TableName)

    sparkSession.streams.awaitAnyTermination(30_000L)
  }

}
