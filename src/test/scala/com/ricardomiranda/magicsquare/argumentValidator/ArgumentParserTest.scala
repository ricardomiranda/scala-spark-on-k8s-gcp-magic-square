package com.ricardomiranda.magicsquare.argumentValidator

import org.scalatest._

class ArgumentParserSpec
  extends wordspec.AnyWordSpec
    with matchers.should.Matchers {

  val wellFormedArguments: Seq[String] = Seq(
    "-m",
    getClass.getResource("/magic_square_empty_file.json").getPath
  )

  val missingFileArguments: Seq[String] = Seq(
    "-m",
    "non_existing_test_file.json"
  )

  "Config parser" should {
    "parse well formed arguments correctly" in {
      val actual: Option[ArgumentParser] =
        ArgumentParser.parser.parse(wellFormedArguments, ArgumentParser())
      assert(actual.isDefined)
    }

    "fail when a JSON file does not exist" in {
      val actual: Option[ArgumentParser] =
        ArgumentParser.parser.parse(missingFileArguments, ArgumentParser())
      assert(actual.isEmpty)
    }
  }
}
