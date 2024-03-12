package com.waitingforcode.demo2_new_columns

import com.waitingforcode.{NumbersWithLettersTable, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, functions}

import java.io.File

object DemoRunner {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(DemoDir))
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(1)
    import sparkSession.implicits._
    println("Creating table...")
    val numbersWithLetters = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("number", "letter")
    numbersWithLetters.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    println("...table created, starting the streaming reader")
    new Thread(() => {
      println("Starting streaming reader")
      StreamingReader.main(args)
    }).start()
    println("Waiting for the streaming reader to process the first micro-batch...")
    while(!StreamingReader.StreamStarted.get()) {}
    println("...processed")

    println("Adding new column...")
    sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} ADD COLUMN upper_letter STRING")
    val numbersWithLettersAndNewColumn = numbersWithLetters
      .withColumn("upper_letter", functions.upper($"letter"))
    numbersWithLettersAndNewColumn.write.format("delta").insertInto(NumbersWithLettersTable)
    println("...new column added")
  }
}
