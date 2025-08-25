package com.waitingforcode.demo4_clone_with_time_travel

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object Demo4 {

  def main(args: Array[String]): Unit = {

    createTable("demo4_src") // 0
    addRecords("demo4_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)) // 1
    addRecords("demo4_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)) // 2
    addRecords("demo4_src", LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333)) // 3

    // Update to invalidate the most recent commit data
    sparkSession.sql("UPDATE demo4_src SET number = number * 10 WHERE number >= 111") // 4

    // Let's clone the table
    DeltaTable.forName("demo4_src").cloneAtVersion(version=3, target="demo4_target", isShallow=true)

    // Here you should only see the added rows from the commit files
    println("Source files")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo4_src/_delta_log/*").show(false)
    println("Target files")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo4_target/_delta_log/*.json").show(false)

    // The table shouldn't contain the updated rows from the UPDATE statement
    println("Target table content at version 3 of the source table")
    sparkSession.read.table("demo4_target").show()
    println("Versus the content of the source table where the most recent version is 4")
    sparkSession.read.table("demo4_src").show()
  }
}
