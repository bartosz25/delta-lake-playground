package com.waitingforcode.demo3_decorators_and_compaction

import com.waitingforcode._
import org.apache.spark.sql.SaveMode

object Demo3 {

  def main(args: Array[String]): Unit = {
    createTable("demo3_src")
    addRecords("demo3_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3))

    // If you create table as .write.format("delta").option("userMetadata", ...).saveAsTable("...")
    // the userMetadata will be ignored because the underlying operation will be different (CREATE TABLE
    // vs. write into Delta when the table exists)
    createTable("demo3_target")
    // This former query
    sparkSession.read.table("demo3_src").write.format("delta").option("userMetadata",
      """
        |{"source": "demo1", "job_version": "v1", "context": "normal_writer"}
        |""".stripMargin).mode(SaveMode.Append).saveAsTable("demo3_target")

    sparkSession.read.table("demo3_src").write.format("delta").option("userMetadata",
      """
        |{"source": "demo1", "job_version": "v2", "context": "normal_writer"}
        |""".stripMargin).mode(SaveMode.Append).saveAsTable("demo3_target")

    sparkSession.read.table("demo3_src").write.format("delta").option("userMetadata",
      """
        |{"source": "demo1", "job_version": "v3", "context": "normal_writer"}
        |""".stripMargin).mode(SaveMode.Append).saveAsTable("demo3_target")

    sparkSession.sql("OPTIMIZE demo3_target")

    sparkSession.sql("DESCRIBE HISTORY demo3_target").show(truncate=false)
    val userMetadata = sparkSession.sql("DESCRIBE HISTORY demo3_target")
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
