package com.waitingforcode.demo6_two_check_constraints

import com.waitingforcode._

import scala.util.Try

object Demo6 {

  def main(args: Array[String]): Unit = {
    createTable("demo6", constraintLetter = "NOT NULL")
    sparkSession.sql(
      """
        |ALTER TABLE demo6 ADD CONSTRAINT numberGreaterThan0 CHECK (number > 0)
        |""".stripMargin)
    sparkSession.sql(
      """
        |ALTER TABLE demo6 ADD CONSTRAINT letterDifferentThanA CHECK (UPPER(letter) != 'A')
        |""".stripMargin)

    // Now, let's try to add an invalid row for the number
    Try(addRecords("demo6", LetterWithNumber("a", -1))).failed.foreach(_.printStackTrace())
    Try(addRecords("demo6", LetterWithNumber("d", -1))).failed.foreach(_.printStackTrace())
  }
}
