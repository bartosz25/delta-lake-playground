package com.waitingforcode.demo1_shallow_clone_delta_table

import com.waitingforcode.{DataWarehouseBaseDir, LetterWithNumber, addRecords, createTable, sparkSession}
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object Demo1 {

  def main(args: Array[String]): Unit = {

    createTable("demo1_src")
    addRecords("demo1_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))
    addRecords("demo1_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33))
    addRecords("demo1_src", LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333))

    // Update to invalidate the most recent commit data
    sparkSession.sql("UPDATE demo1_src SET number = number * 10 WHERE number >= 111")

    // Let's clone the table
    DeltaTable.forName("demo1_src").clone(target="demo1_target", isShallow=true)

    // Here you should only see the added rows from the commit files
    println("Source files")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo1_src/_delta_log/*").show(false)
    println("Target files")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo1_target/_delta_log/*.json").show(false)

    // Let's now change some data in both source and target tables:
    sparkSession.sql("UPDATE demo1_src SET number = number + 5 WHERE number >= 111")
    sparkSession.sql("UPDATE demo1_target SET number = number * 10 WHERE number >= 111")

    // Both tables will now diverge
    println("Source table content")
    sparkSession.read.table("demo1_src").show()
    println("Target table content")
    sparkSession.read.table("demo1_target").show()

    // Now we can drop the content of the cloned table without impacting the source table
    DeltaTable.forName("demo1_target").delete()
    println("Source table content")
    sparkSession.read.table("demo1_src").show()
    println("Target table content")
    sparkSession.read.table("demo1_target").show()

    // However, the opposite is not true
    DeltaTable.forName("demo1_src").clone(target="demo1_target_new", isShallow=true)
    println("Content of the cloned table before the delete of the original table")
    sparkSession.read.table("demo1_target_new").show()
    println("Content of the cloned table after the delete of the original one")
    FileUtils.deleteDirectory(new File(s"${DataWarehouseBaseDir}/demo1_src"))
    sparkSession.read.table("demo1_target_new").show()

  }
}
