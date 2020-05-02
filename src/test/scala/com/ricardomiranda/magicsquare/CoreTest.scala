package com.ricardomiranda.magicsquare

import org.scalatest._

class CoreTest extends wordspec.AnyWordSpec with matchers.should.Matchers {

  "get file contents" should {
    "return None when getting file contents when the file doesn't exist" in {
      val actual = Core
        .getFileContents(filePath = "non_existent_file.json")
      actual shouldEqual None
    }

    "return file contents when getting file contents when the file exist" in {
      val actual = Core
        .getFileContents(filePath =
          getClass.getResource("/magic_square_empty_file.json").getPath
        )
      actual shouldEqual Some("{}")
    }
  }
}
