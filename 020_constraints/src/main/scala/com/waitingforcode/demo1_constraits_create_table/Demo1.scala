package com.waitingforcode.demo1_constraits_create_table

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File
import scala.util.Try

object Demo1 {

  def main(args: Array[String]): Unit = {
    createTable("demo1", constraintLetter = "NOT NULL")
    sparkSession.sql(
      """
        |ALTER TABLE demo1 ADD CONSTRAINT numberGreaterThan0 CHECK (number > 0)
        |""".stripMargin)
    addRecords("demo1", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))

    sparkSession.read.table("demo1").show()

    // Now, let's try to add an invalid row for the number
    Try(addRecords("demo1", LetterWithNumber(null, 1))).failed.foreach(_.printStackTrace())
    Try(addRecords("demo1", LetterWithNumber("d", -1))).failed.foreach(_.printStackTrace())
  }
}
