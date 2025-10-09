package com.waitingforcode.demo1_retries_with_fanout

import com.waitingforcode._
import org.apache.spark.sql.{DataFrame, functions}

object Demo1 {

  def main(args: Array[String]): Unit = {
    createTable("demo1_1")
    createTable("demo1_2")
    def writeWithFanout(dataFrame1: DataFrame, dataFrame2: DataFrame,
                        appId: String, transactionVersion: String,
                        shouldFail: Boolean) = {
      val fanoutOptions = Map(
        "txnVersion" -> transactionVersion, "txnAppId" -> appId,
        "mergeSchema" -> "true"
      )
      dataFrame1.withColumns(Map(
        "txnVersion" -> functions.lit(transactionVersion), "txnAppId" -> functions.lit(appId)
      )).write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo1_1")

      if (shouldFail) {
        throw new RuntimeException("Error!")
      }
      dataFrame2.withColumns(Map(
        "txnVersion" -> functions.lit(transactionVersion), "txnAppId" -> functions.lit(appId)
      )).write.format("delta").mode("append").options(fanoutOptions).saveAsTable("demo1_2")
    }

    printBoldText("Trying to insert records with failure after the second writer...")
    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    val df2 = Seq(LetterWithNumber("A", 1), LetterWithNumber("B", 2), LetterWithNumber("C", 3)).toDF()
    try {
      writeWithFanout(dataFrame1 = df1, dataFrame2 = df2, appId = "app_1", transactionVersion = "1",
        shouldFail = true)
    } catch {
      case e: RuntimeException => println("Caught planned exception")
    }
    sparkSession.read.table("demo1_1").show(truncate=false)
    sparkSession.read.table("demo1_2").show(truncate=false)

    printBoldText("Retrying the write without errors this time...")
    writeWithFanout(dataFrame1 = df1, dataFrame2 = df2, appId = "app_1", transactionVersion = "1",
      shouldFail = false)
    sparkSession.read.table("demo1_1").show(truncate=false)
    sparkSession.read.table("demo1_2").show(truncate=false)

    printBoldText("Writing with the same appId and transaction version...")
    writeWithFanout(dataFrame1 = df1, dataFrame2 = df2, appId = "app_1", transactionVersion = "1",
      shouldFail = false)
    sparkSession.read.table("demo1_1").show(truncate=false)
    sparkSession.read.table("demo1_2").show(truncate=false)

    printBoldText("Writing with the same appId but different transaction version...")
    val df1_new = Seq(LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)).toDF()
    val df2_new = Seq(LetterWithNumber("A", 11), LetterWithNumber("B", 22), LetterWithNumber("C", 33)).toDF()
    writeWithFanout(dataFrame1 = df1_new, dataFrame2 = df2_new, appId = "app_1", transactionVersion = "2",
      shouldFail = false)
    sparkSession.read.table("demo1_1").show(truncate=false)
    sparkSession.read.table("demo1_2").show(truncate=false)

    printBoldText("Writing with different appId but the same transaction version...")
    val df1_newer = Seq(LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333)).toDF()
    val df2_newer = Seq(LetterWithNumber("A", 111), LetterWithNumber("B", 222), LetterWithNumber("C", 333)).toDF()
    writeWithFanout(dataFrame1 = df1_newer, dataFrame2 = df2_newer, appId = "app_2", transactionVersion = "2",
      shouldFail = false)
    sparkSession.read.table("demo1_1").show(truncate=false)
    sparkSession.read.table("demo1_2").show(truncate=false)

    printBoldText("Showing transaction logs for both tables")
    Seq("demo1_1", "demo1_2").foreach(table => {
      sparkSession.sql(s"DESCRIBE HISTORY ${table}").show(truncate=false)
      sparkSession.read.text(s"${DataWarehouseBaseDir}/${table}/_delta_log/*")
        .filter("value LIKE '%appId%'").show(truncate=false)
    })
  }
}
