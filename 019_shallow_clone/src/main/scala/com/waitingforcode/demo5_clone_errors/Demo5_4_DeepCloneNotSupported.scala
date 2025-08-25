package com.waitingforcode.demo5_clone_errors

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object Demo5_4_DeepCloneNotSupported {

  def main(args: Array[String]): Unit = {
    createTable("demo5_3_src") // 0
    addRecords("demo5_3_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)) // 1

    DeltaTable.forName("demo5_3_src").clone(target="demo5_3_target", isShallow=false)
  }
}
