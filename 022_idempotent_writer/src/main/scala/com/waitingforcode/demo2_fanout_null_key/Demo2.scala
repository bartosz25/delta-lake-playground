package com.waitingforcode.demo2_fanout_null_key

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, functions}

import java.io.File

object Demo2 {

  def main(args: Array[String]): Unit = {
    createTable("demo2_1")
    createTable("demo2_2")
    def writeWithFanout(dataFrame1: DataFrame, dataFrame2: DataFrame,
                        transactionVersion: String) = {
      val fanoutOptions = Map(
        "txnVersion" -> transactionVersion, "txnAppId" -> null
      )
      dataFrame1.write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo2_1")
      dataFrame2.write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo2_2")
    }

    printBoldText("null appId is a valid application id, as long as it's present in the options, " +
      "Delta writer won't complain")
    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    val df2 = Seq(LetterWithNumber("A", 1), LetterWithNumber("B", 2), LetterWithNumber("C", 3)).toDF()
    writeWithFanout(dataFrame1 = df1, dataFrame2 = df2, transactionVersion = "1")
    sparkSession.read.table("demo2_1").show(truncate=false)
    sparkSession.read.table("demo2_2").show(truncate=false)

    printBoldText("Retrying with the same transaction version")
    writeWithFanout(dataFrame1 = df1, dataFrame2 = df2, transactionVersion = "1")
    sparkSession.read.table("demo2_1").show(truncate=false)
    sparkSession.read.table("demo2_2").show(truncate=false)

    printBoldText("Now, imagine there is another job with the null appId, its data won't be written!")
    val df1_2 = Seq(LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)).toDF()
    val df2_2 = Seq(LetterWithNumber("A", 11), LetterWithNumber("B", 22), LetterWithNumber("C", 33)).toDF()

    writeWithFanout(dataFrame1 = df1_2, dataFrame2 = df2_2, transactionVersion = "1")
    sparkSession.read.table("demo2_1").show(truncate=false)
    sparkSession.read.table("demo2_2").show(truncate=false)
  }
}
