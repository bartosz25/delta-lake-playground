package com.waitingforcode.demo6_partitioned_table

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object Demo6 {

  def main(args: Array[String]): Unit = {

    createTable("demo6_src", "PARTITIONED BY (letter)")
    addRecords("demo6_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))
    addRecords("demo6_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33))
    addRecords("demo6_src", LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333))

    // Let's clone the table
    DeltaTable.forName("demo6_src").clone(target="demo6_target", isShallow=true)

    // Here you should only see the added rows from the commit files and the "metaData" entry should
    // have the partitionColumns set to ["letter"]
    println("Target files")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo6_target/_delta_log/*.json").show(false)

    println("Target table content")
    sparkSession.read.table("demo6_target").show()
  }
}
