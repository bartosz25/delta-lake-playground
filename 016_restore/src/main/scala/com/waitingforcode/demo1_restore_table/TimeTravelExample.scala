package com.waitingforcode.demo1_restore_table

import com.waitingforcode.{NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object TimeTravelExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    import sparkSession.implicits._

    sparkSession.sql(s"DESCRIBE HISTORY default.${NumbersWithLettersTable}").show(truncate=false)

    println("--- Printing all records ---")
    sparkSession.sql(s"SELECT * FROM ${NumbersWithLettersTable} ORDER BY letter").show(truncate=false)

    println("--- Printing records for the version 1 ---")
    sparkSession.sql(s"SELECT * FROM ${NumbersWithLettersTable} VERSION AS OF 1 ORDER BY letter").show(truncate=false)

    println("--- Printing all records after time travel ---")
    sparkSession.sql(s"SELECT * FROM ${NumbersWithLettersTable} ORDER BY letter").show(truncate=false)
  }

}
