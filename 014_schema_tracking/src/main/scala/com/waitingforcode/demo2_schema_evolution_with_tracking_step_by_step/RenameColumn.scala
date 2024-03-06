package com.waitingforcode.demo2_schema_evolution_with_tracking_step_by_step

import com.waitingforcode.NumbersWithLettersTable
import org.apache.spark.sql.SparkSession

object RenameColumn {

  def run(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} RENAME COLUMN letter TO letter2")
    val numbersWithLetters2 = Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d")
    ).toDF("number", "letter2")
    numbersWithLetters2.write.format("delta").insertInto(NumbersWithLettersTable)
  }

}
