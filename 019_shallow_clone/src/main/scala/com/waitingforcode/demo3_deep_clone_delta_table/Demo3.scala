package com.waitingforcode.demo3_deep_clone_delta_table

import com.waitingforcode.{DataWarehouseBaseDir, LetterWithNumber, addRecords, createTable, sparkSession}
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.delta.DeltaLog

import java.io.File

object Demo3 {

  def main(args: Array[String]): Unit = {
    createTable("demo3_src", "PARTITIONED BY (letter)")
    addRecords("demo3_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))
    addRecords("demo3_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33))

    // CTAS doesn't work as it doesn't preserve the partitioning!
    //sparkSession.sql("CREATE TABLE demo3_target USING DELTA AS SELECT * FROM demo3_src")
    // That's why we need to start by creating the table "LIKE" the source table
    sparkSession.sql("CREATE TABLE demo3_target   LIKE demo3_src")
    // At this moment the table should be empty but it should preserve the partition columns
    sparkSession.read.table("demo3_target").show()
    DeltaTable.forName("demo3_target").detail().show()

    // Now we can read the data from the source table and write it to the output
    // Of course, this approach requires some boilerplate code. For that reason if you are on Databricks
    // you should prefer the native deep clone capability
    sparkSession.read.table("demo3_src").writeTo("demo3_target").append()
    sparkSession.read.table("demo3_target").show()
    DeltaTable.forName("demo3_target").detail().show()
  }

}
