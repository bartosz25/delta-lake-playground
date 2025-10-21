package com.waitingforcode.demo1_add_decorators_by_default

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions

import java.io.File

object Demo1 {

  def main(args: Array[String]): Unit = {
    sparkSession.conf.set("spark.databricks.delta.commitInfo.userMetadata",
      """
        |{"source": "demo1", "job_version": "v1", "context": "normal_writer"}
        |""".stripMargin.trim())
    createTable("demo1_src")
    addRecords("demo1_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))
    addRecords("demo1_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33))
    addRecords("demo1_src", LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333))

    val userMetadata = sparkSession.sql("DESCRIBE HISTORY demo1_src")
      .selectExpr("PARSE_JSON(userMetadata) AS userMetadata")
    userMetadata.printSchema()
    userMetadata.show(false)
    //userMetadata.select(functions.variant_get($""))
    userMetadata.selectExpr(
        "variant_get(userMetadata, '$.context') AS context", "variant_get(userMetadata, '$.job_version') AS job_version",
        "variant_get(userMetadata, '$.source') AS source"
      )
      .show()
  }
}
