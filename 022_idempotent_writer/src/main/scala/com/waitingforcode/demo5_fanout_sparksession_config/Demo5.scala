package com.waitingforcode.demo5_fanout_sparksession_config

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, functions}

import java.io.File

object Demo5 {

  def main(args: Array[String]): Unit = {
    createTable("demo1_1")
    createTable("demo1_2")

    sparkSession.conf.set("spark.databricks.delta.write.txnAppId", "demo5")
    sparkSession.conf.set("spark.databricks.delta.write.txnVersion", "1")
    sparkSession.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", true)
    def writeWithFanout(dataFrame1: DataFrame, dataFrame2: DataFrame) = {
      dataFrame1.write.format("delta").mode("append").saveAsTable("demo1_1")
      dataFrame2.write.format("delta").mode("append").saveAsTable("demo1_2")
    }
    printBoldText("Writing the rows for the first time should fail because the autoReset.enabled removes the txnVersion")
    import sparkSession.implicits._
    val df1 = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDF()
    val df2 = Seq(LetterWithNumber("A", 1), LetterWithNumber("B", 2), LetterWithNumber("C", 3)).toDF()
    try {
      writeWithFanout(dataFrame1 = df1, dataFrame2 = df2)
    } catch {
      case e: RuntimeException => println("Caught planned exception")
    }
    sparkSession.read.table("demo1_1").show(truncate=false)
    sparkSession.read.table("demo1_2").show(truncate=false)
  }
}
