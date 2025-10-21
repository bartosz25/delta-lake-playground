package com.waitingforcode.demo2_failing_truncate

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, functions}

import java.io.File

object Demo2 {

  def main(args: Array[String]): Unit = {
    createTable("demo2_1")
    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    df1.write.format("delta").mode("append").saveAsTable("demo2_1")

    sparkSession.sql("TRUNCATE TABLE demo2_1")
  }
}
