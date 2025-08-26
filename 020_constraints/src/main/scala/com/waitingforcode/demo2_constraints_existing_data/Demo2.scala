package com.waitingforcode.demo2_constraints_existing_data

import com.waitingforcode._

import scala.util.Try

object Demo2 {

  def main(args: Array[String]): Unit = {
    createTable("demo2", constraintLetter = "NOT NULL")
    addRecords("demo2", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))
    sparkSession.read.table("demo2").show()
    sparkSession.sql(
      """
        |ALTER TABLE demo2 ADD CONSTRAINT numberGreaterThan10 CHECK (number > 10)
        |""".stripMargin)
  }
}
