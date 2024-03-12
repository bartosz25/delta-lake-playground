package com.waitingforcode.demo4_max_records

import com.waitingforcode.{DataWarehouseBaseDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.spark.sql.delta.DeltaLog

object TableReader {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(3)

    val deltaLog = DeltaLog.forTable(sparkSession, s"${DataWarehouseBaseDir}/${TableName}")

    import sparkSession.implicits._
    deltaLog.unsafeVolatileSnapshot.allFiles.orderBy($"path".asc).show(5, false)

    println("All files="+deltaLog.unsafeVolatileSnapshot.allFiles.count())

  }

}
