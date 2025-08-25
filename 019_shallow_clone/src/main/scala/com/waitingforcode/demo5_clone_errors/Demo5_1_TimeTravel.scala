package com.waitingforcode.demo5_clone_errors

import com.waitingforcode._
import io.delta.tables.DeltaTable

object Demo5_1_TimeTravel {

  def main(args: Array[String]): Unit = {
    createTable("demo5_1_src") // 0
    addRecords("demo5_1_src", LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)) // 1
    addRecords("demo5_1_src", LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)) // 2
    addRecords("demo5_1_src", LetterWithNumber("a", 111), LetterWithNumber("b", 222), LetterWithNumber("c", 333)) // 3

    DeltaTable.forName("demo5_1_src").clone(target="demo5_1_target", isShallow=true)

    // Cloned table has its own timeline because it writes the active data files for the last snapshot version
    // Therefore, you won't be able to time travel as if you would do in the source table
    // Below the time travel for the source table will work but it will fail for the cloned table because the single
    // available version is 0
    println("Source table at version 2")
    sparkSession.sql("SELECT * FROM demo5_1_src VERSION AS OF 2").show()
    println("Target table at version 2")
    sparkSession.sql("SELECT * FROM demo5_1_target VERSION AS OF 2").show()
  }
}
