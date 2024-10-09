package com.waitingforcode.demo1_restore_table

import com.waitingforcode.{NumbersWithLettersTable, getOrCreateSparkSessionWithDeltaLake}

object RestoreExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()

    sparkSession.sql(s"DESCRIBE HISTORY default.${NumbersWithLettersTable}").show(truncate=false)

    println("--- Printing all records ---")
    sparkSession.sql(s"SELECT * FROM ${NumbersWithLettersTable} ORDER BY letter").show(truncate=false)
    
    println("--- Restoring the table to the version 1 ---")
    sparkSession.sql(s"RESTORE TABLE ${NumbersWithLettersTable} TO VERSION AS OF 1")

    println("--- Printing all records after the restore ---")
    sparkSession.sql(s"SELECT * FROM ${NumbersWithLettersTable} ORDER BY letter").show(truncate=false)

    println("--- Printing the history ---")
    sparkSession.sql(s"DESCRIBE HISTORY default.${NumbersWithLettersTable}").show(truncate=false)
  }

}
