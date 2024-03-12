package com.waitingforcode.demo1_idempotent_fan_out

import org.apache.spark.sql.DataFrame

object IdempotentForeachBatchWriter {

   def getWriter(shouldFail: Boolean, shouldBeIdempotent: Boolean): (DataFrame, Long) => Unit = {
     (dataset: DataFrame, batchVersion: Long) => {
       val writeOptions: Map[String, String] = if (!shouldBeIdempotent) {
         Map.empty
       } else {
         Map(
           "txnVersion" -> batchVersion.toString, "txnAppId" -> AppId
         )
       }

       println("Writing 2 tables...")
       dataset.write.format("delta").mode("append").options(writeOptions).save(Table1Location)

       if (shouldFail) {
         throw new RuntimeException("An error occurred before writing table 2")
       }

       dataset.write.format("delta").mode("append").options(writeOptions).save(Table2Location)
       println("...written 2 tables")
       ()
     }
  }
}
