package com.waitingforcode.demo5_clone_errors

import com.waitingforcode._
import io.delta.tables.DeltaTable

object Demo5_2_ExistingClone {

  def main(args: Array[String]): Unit = {
    createTable("demo5_2_src") // 0
    addRecords("demo5_2_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)) // 1
    addRecords("demo5_2_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)) // 2
    addRecords("demo5_2_src", LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333)) // 3

    DeltaTable.forName("demo5_2_src").clone(target="demo5_2_target", isShallow=true, replace=false)
    DeltaTable.forName("demo5_2_src").clone(target="demo5_2_target", isShallow=true, replace=false)
  }
}
