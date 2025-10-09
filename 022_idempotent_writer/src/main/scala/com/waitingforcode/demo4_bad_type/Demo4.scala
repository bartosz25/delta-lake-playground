package com.waitingforcode.demo4_bad_type

import com.waitingforcode.{LetterWithNumber, createTable, printBoldText, sparkSession}
import org.apache.spark.sql.DataFrame

object Demo4 {

  def main(args: Array[String]): Unit = {
    printBoldText("Here the writer will fail because the transaction version is supposed to be a numeric value")
    createTable("demo4_1")
    createTable("demo4_2")
    def writeWithFanout(dataFrame1: DataFrame, dataFrame2: DataFrame, transactionVersion: String) = {
      val fanoutOptions = Map(
        "txnVersion" -> transactionVersion, "txnAppId" -> "app1"
      )
      dataFrame1.write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo4_1")
      dataFrame2.write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo4_2")
    }
    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    val df2 = Seq(LetterWithNumber("A", 1), LetterWithNumber("B", 2), LetterWithNumber("C", 3)).toDF()
    writeWithFanout(dataFrame1 = df1, dataFrame2 = df2, transactionVersion = "transaction_1")
  }
}
