package com.waitingforcode.demo3_table_overwrite

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, functions}

import java.io.File

object Demo3 {

  def main(args: Array[String]): Unit = {
    createTable("demo3_1")
    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    df1.write.format("delta").mode("append").saveAsTable("demo3_1")

    val df2 = Seq(LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)).toDF()
    df2.write.format("delta").mode("overwrite").saveAsTable("demo3_1")
  }
}
