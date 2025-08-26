package com.waitingforcode.demo3_complex_constraints

import com.waitingforcode._

import scala.util.Try

object Demo3 {

  def main(args: Array[String]): Unit = {
    createTable("demo3", constraintLetter = "NOT NULL")
    sparkSession.sql(
      """
        |ALTER TABLE demo3 ADD CONSTRAINT numberGreaterThan0 CHECK
        | (CASE WHEN number = 0 THEN letter != 'x' ELSE letter != '..' END)
        |""".stripMargin)
    addRecords("demo3", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))

    sparkSession.read.table("demo3").show()

    Try(addRecords("demo3", LetterWithNumber("x", 0))).failed.foreach(_.printStackTrace())
    Try(addRecords("demo3", LetterWithNumber("..", 1))).failed.foreach(_.printStackTrace())
  }
}
