package com.waitingforcode.demo5_clone_errors

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object Demo5_3_DeletedSourceFiles {

  def main(args: Array[String]): Unit = {
    createTable("demo5_3_src") // 0
    addRecords("demo5_3_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)) // 1
    addRecords("demo5_3_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)) // 2
    addRecords("demo5_3_src", LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333)) // 3

    DeltaTable.forName("demo5_3_src").clone(target="demo5_3_target", isShallow=true)
    sparkSession.read.table("demo5_3_target").show()

    FileUtils.deleteDirectory(new File(s"${DataWarehouseBaseDir}/demo5_3_src"))
    sparkSession.read.table("demo5_3_target").show()
  }
}
