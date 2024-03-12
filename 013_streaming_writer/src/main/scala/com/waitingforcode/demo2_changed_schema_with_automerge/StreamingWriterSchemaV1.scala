package com.waitingforcode.demo2_changed_schema_with_automerge

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake
import org.apache.commons.io.FileUtils

import java.io.File

object StreamingWriterSchemaV1 {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(DemoDir))
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(2)

    val rateStreamInput = getRateStreamSource(sparkSession)

    rateStreamInput.writeStream.option("checkpointLocation", CheckpointLocation)
      .format("delta").toTable(TableName)

    sparkSession.streams.awaitAnyTermination(30_000L)
  }

}
