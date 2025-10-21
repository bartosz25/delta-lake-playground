package com.waitingforcode.demo1_simulate_truncate

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, functions}

import java.io.File

object Demo1 {

  def main(args: Array[String]): Unit = {
    createTable("demo1_1")
    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    df1.write.format("delta").mode("append").saveAsTable("demo1_1")

    sparkSession.sql("DELETE FROM demo1_1")
  }
}
