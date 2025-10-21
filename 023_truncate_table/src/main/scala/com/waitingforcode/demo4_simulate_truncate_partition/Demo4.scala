package com.waitingforcode.demo4_simulate_truncate_partition

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, functions}

import java.io.File

object Demo4 {

  def main(args: Array[String]): Unit = {
    createTable("demo4", "PARTITIONED BY (letter)")
    import sparkSession.implicits._
    Seq(
      Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)),
      Seq(LetterWithNumber("a", 10), LetterWithNumber("b", 20), LetterWithNumber("c", 30)),
      Seq(LetterWithNumber("a", 100), LetterWithNumber("b", 200), LetterWithNumber("c", 300)),
    ).foreach(lettersToWrite => {
      println("Writing a DataFrame...")
      lettersToWrite.toDF().write.format("delta").mode("append").saveAsTable("demo4")
    })
    println("Deleting a partition")
    sparkSession.sql("DELETE FROM demo4 WHERE letter = 'a'")
    sparkSession.sql("DESCRIBE HISTORY demo4").show(truncate=false)
  }
}
