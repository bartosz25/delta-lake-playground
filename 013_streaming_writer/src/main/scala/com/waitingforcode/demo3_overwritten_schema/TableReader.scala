package com.waitingforcode.demo3_overwritten_schema

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake

object TableReader {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    import sparkSession.implicits._
    sparkSession.read.format("delta").table(TableName).orderBy($"value".asc).show(5, false)
  }

}
