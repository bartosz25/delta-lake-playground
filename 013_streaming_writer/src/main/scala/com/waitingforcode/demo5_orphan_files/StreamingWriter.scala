package com.waitingforcode.demo5_orphan_files

import com.waitingforcode.{DataWarehouseBaseDir, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.Trigger

import java.io.File
import java.nio.file.{FileSystems, Paths, StandardWatchEventKinds}
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.CollectionHasAsScala

object StreamingWriter {

  val DemoDir = s"${OutputDir}/demo5"
  val CheckpointLocation = s"${DemoDir}/checkpoint"
  val TableName ="demo5_table"

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(DemoDir))
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(8)
    import sparkSession.implicits._
    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()
      .map(row => {
       //  Thread.sleep(5_000L)
        (1, 2, 3)
      }).toDF("a", "b", "c")

    val tablePath = Paths.get(s"${DataWarehouseBaseDir}/${TableName}")
    tablePath.toFile.mkdirs()
    val watcher = FileSystems.getDefault().newWatchService()
    val key = tablePath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
    new Thread(() => {
      while (true) {
        key.pollEvents().forEach(event => {
          FileUtils.delete(new File(s"${tablePath.toString}/${event.context()}"))
          println(event.kind().name() + " ==> " + event.context())
        })
      }
    }).start()

    try {
      rateStreamInput.writeStream
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", CheckpointLocation)
        .option("maxRecordsPerFile", 1)
        .option("compression", "gzip")
        .format("delta")
        .toTable(TableName)

      sparkSession.streams.awaitAnyTermination()
    } finally {
      FileUtils.listFiles(tablePath.toFile, null, false).asScala.foreach(file => {
        println(s"Create data file=${file.getAbsolutePath}")
      })
    }
  }
}
