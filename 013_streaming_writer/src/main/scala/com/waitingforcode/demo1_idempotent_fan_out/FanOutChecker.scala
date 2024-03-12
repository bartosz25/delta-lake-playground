package com.waitingforcode.demo1_idempotent_fan_out

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake

object FanOutChecker {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()

    import sparkSession.implicits._
    sparkSession.read.format("delta").load(Table1Location).groupBy($"timestamp", $"value")
      .count().filter("count > 1").orderBy($"value".asc).show(10, false)
    println(s"All records in the ${Table1Location}=${sparkSession.read.format("delta").load(Table1Location).count()}")
    sparkSession.read.format("delta").load(Table2Location).groupBy($"timestamp", $"value")
      .count().filter("count > 1").show(false)
  }

}
