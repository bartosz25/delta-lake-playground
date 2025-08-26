package com.waitingforcode.demo4_multiple_constraints_violations

import com.waitingforcode._

import scala.util.Try

object Demo4 {

  def main(args: Array[String]): Unit = {
    createTable("demo4", constraintLetter = "NOT NULL")
    sparkSession.sql(
      """
        |ALTER TABLE demo4 ADD CONSTRAINT numberGreaterThan0 CHECK (number > 0)
        |""".stripMargin)
    addRecords("demo4", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))

    sparkSession.read.table("demo4").show()


    Try(addRecords("demo4", LetterWithNumber("d", 4), LetterWithNumber("e", 5), LetterWithNumber("f", 6),
      LetterWithNumber(null, 1), LetterWithNumber("d", -1))).failed.foreach(_.printStackTrace())
    // Let's now reverse the error rows and put the row violating the number constraint before the row
    // violating the nullability constraint
    Try(addRecords("demo4", LetterWithNumber("d", 4), LetterWithNumber("e", 5), LetterWithNumber("f", 6),
      LetterWithNumber("d", -1), LetterWithNumber(null, 1))).failed.foreach(_.printStackTrace())

    sparkSession.read.table("demo4").show()
  }
}
