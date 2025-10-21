package com.waitingforcode.demo5_decorator_optimize

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, functions}

import java.io.File

object Demo5 {

  def main(args: Array[String]): Unit = {
    createTable("demo5")
    import sparkSession.implicits._
    val letters = Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))
    letters.toDS().write.format("delta").mode(SaveMode.Append).option("maxRecordsPerFile", 1).saveAsTable("demo5")
    letters.toDS().write.format("delta").mode(SaveMode.Append).option("maxRecordsPerFile", 1).saveAsTable("demo5")

    sparkSession.conf.set("spark.databricks.delta.commitInfo.userMetadata",
      """
        |{"source": "demo5", "job_version": "v1", "context": "regular_optimize"}
        |""".stripMargin.trim())
    DeltaTable.forName("demo5").optimize().executeCompaction()

    val userMetadata = sparkSession.sql("DESCRIBE HISTORY demo5")
      .selectExpr("PARSE_JSON(userMetadata) AS userMetadata")
    userMetadata.show(false)
    userMetadata.selectExpr(
        "variant_get(userMetadata, '$.context') AS context", "variant_get(userMetadata, '$.job_version') AS job_version",
        "variant_get(userMetadata, '$.source') AS source"
      )
      .show()
  }
}
