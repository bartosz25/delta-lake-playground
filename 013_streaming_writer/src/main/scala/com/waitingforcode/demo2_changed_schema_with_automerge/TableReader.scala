package com.waitingforcode.demo2_changed_schema_with_automerge

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake

object TableReader {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()

    sparkSession.read.format("delta").table(TableName).show(5, false)
  }

}
