package com.waitingforcode.demo5_orphan_files

import com.waitingforcode._

import scala.util.Try

object Demo5 {

  def main(args: Array[String]): Unit = {
    createTable("demo5", constraintLetter = "NOT NULL")
    sparkSession.sql(
      """
        |ALTER TABLE demo5 ADD CONSTRAINT numberGreaterThan0 CHECK (number > 0)
        |""".stripMargin)
    addRecords("demo5", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))
    sparkSession.read.table("demo5").show()
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo5/_delta_log/*").show(false)

    import sparkSession.implicits._
    Try(
      Seq(LetterWithNumber("d", 4), LetterWithNumber("e", 5), LetterWithNumber("f", 6),
        LetterWithNumber(null, 1), LetterWithNumber("d", -1)).toDS().repartition(1)
        .orderBy($"letter".desc_nulls_last).writeTo("demo5") // order by to validate the valid records before
        .option("maxRecordsPerFile", 1).append() // ensure we write only 1 row/file to see the partial write
    ).failed.foreach(_.printStackTrace())

    sparkSession.read.table("demo5").show()

    // There shouldn't be new commits...
    println("Commit log content")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo5/_delta_log/*").show(false)
    // ...but you should see partial data with the valid rows written before the writing process failed
    println("Parquet files content")
    sparkSession.read.parquet(s"${DataWarehouseBaseDir}/demo5/").show(false)
    /**
     * Expected:
     +------+------+
     |letter|number|
     +------+------+
     |a     |1     |
     |b     |2     |
     |c     |3     |
     |d     |4     |
     |e     |5     |
     |f     |6     |
     +------+------+
     */
  }
}
