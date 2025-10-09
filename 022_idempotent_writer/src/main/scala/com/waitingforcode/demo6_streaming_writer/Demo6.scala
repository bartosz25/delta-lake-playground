package com.waitingforcode.demo6_streaming_writer

import com.waitingforcode._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, functions}

import java.io.File

object Demo6 {

  private def writeNewFile(fileName: String): Unit = {
    FileUtils.writeStringToFile(new File(s"/tmp/idempotent_writer_data/${fileName}"), s"${fileName} content", "UTF-8")
  }

  def main(args: Array[String]): Unit = {
    writeNewFile("file1.txt")
    writeNewFile("file2.txt")
    val rateStreamInput = sparkSession.readStream.format("text")
      .load("/tmp/idempotent_writer_data/")

    val query = rateStreamInput.writeStream
      .option("checkpointLocation", "/tmp/checkpoint_demo6")
      .format("delta")
      .option("txnVersion", "1")
      .option("txnAppId", "app1")
      .queryName("query_app1")
      .toTable("demo6")

    query.processAllAvailable()
    def select(): Unit = {
      sparkSession.read.table(s"demo6").show(truncate=false)
      sparkSession.sql(s"DESCRIBE HISTORY demo6").show(truncate=false)
      val userMetadata = sparkSession.sql(s"DESCRIBE HISTORY demo6")
      userMetadata.printSchema()
      userMetadata.show(false)
    }

    printBoldText("After processing the first micro-batch")
    select()

    printBoldText("Writing new file")
    writeNewFile("file3.txt")
    writeNewFile("file4.txt")
    query.processAllAvailable()
    select()

    query.stop()

    printBoldText("Removing files from the last checkpoint to see if the idempotent writer works" +
      "After all, we're going to reprocess an already processed micro-batch")
    FileUtils.delete(new File("/tmp/checkpoint_demo6/sources/0/1"))
    FileUtils.delete(new File("/tmp/checkpoint_demo6/sources/0/.1.crc"))
    FileUtils.delete(new File("/tmp/checkpoint_demo6/offsets/1"))
    FileUtils.delete(new File("/tmp/checkpoint_demo6/offsets/.1.crc"))
    FileUtils.delete(new File("/tmp/checkpoint_demo6/commits/1"))
    FileUtils.delete(new File("/tmp/checkpoint_demo6/commits/.1.crc"))
    assert(!new File("/tmp/checkpoint_demo6/sources/0/1").exists())
    writeNewFile("file5.txt")
    printBoldText("Starting new query from the checkpoint")
    val rateStreamInput2 = sparkSession.readStream.format("text")
      .load("/tmp/idempotent_writer_data/")


    val query2 = rateStreamInput2.writeStream
      .option("checkpointLocation", "/tmp/checkpoint_demo6")
      .format("delta")
      .option("txnVersion", "1")
      .option("txnAppId", "app1")
      .queryName("query_app1")
      .toTable("demo6")
    query2.processAllAvailable()
    select()

    printBoldText("You shouldn't see the file5.txt integrated because the micro-batch was skipped")
    println("You can see that by checking the logs with:" +
      "less /tmp/delta-lake-playground/022/log.out | grep \"Skipping already\"" +
      "Skipping already complete epoch 1, in query 6898e123-e596-43bb-af6a-9deee942d998 (org.apache.spark.sql.delta.sources.DeltaSink:190)")
  }
}


object Demo6AfterRestart {


  def main(args: Array[String]): Unit = {
    val rateStreamInput = sparkSession.readStream.format("text")
      .load("/tmp/idempotent_writer_data/")


    val query = rateStreamInput.writeStream
      .option("checkpointLocation", "/tmp/checkpoint_demo6")
      .format("delta")
      .option("txnVersion", "1")
      .option("txnAppId", "app1")
      .queryName("query_app1")
      .toTable("demo6")
    query.processAllAvailable()

    def select(): Unit = {
      sparkSession.read.table(s"demo6").show(truncate = false)
      sparkSession.sql(s"DESCRIBE HISTORY demo6").show(truncate = false)
      val userMetadata = sparkSession.sql(s"DESCRIBE HISTORY demo6")
      userMetadata.printSchema()
      userMetadata.show(false)
    }

    printBoldText("After processing the first micro-batch")
    select()
  }
}