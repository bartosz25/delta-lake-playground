package com.waitingforcode.demo3_fanout_missing_entries

import com.waitingforcode._
import org.apache.spark.sql.DataFrame

object Demo3MissingTransactionId {

  def main(args: Array[String]): Unit = {
    printBoldText("Here the writer will fail because the idempotent writer expects both app and transaction to be specified")
    createTable("demo3_2_1")
    createTable("demo3_2_2")
    def writeWithFanout(dataFrame1: DataFrame, dataFrame2: DataFrame, appId: String) = {
      val fanoutOptions = Map("txnAppId" -> appId)
      dataFrame1.write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo3_2_1")
      dataFrame2.write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo3_2_2")
    }

    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    val df2 = Seq(LetterWithNumber("A", 1), LetterWithNumber("B", 2), LetterWithNumber("C", 3)).toDF()
      writeWithFanout(dataFrame1 = df1, dataFrame2 = df2, appId = "app1")
  }
}
