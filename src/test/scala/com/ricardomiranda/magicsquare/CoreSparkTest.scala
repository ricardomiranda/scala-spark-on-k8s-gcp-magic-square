package com.ricardomiranda.magicsquare

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest._

class CoreSparkTest extends funsuite.AnyFunSuite with DataFrameSuiteBase {
  test(testName = "Compute hashed_dataframe") {
    val struct: StructType = StructType(
      Seq(
        StructField("chromosome", ArrayType(LongType, false), true),
        StructField("fitness", LongType, false)
      )
    )
    val filePath: String = getClass.getResource("/population_10.json").getPath
    val filePopulation: DataFrame = spark.read.schema(struct).json(filePath)
    val actual: Int = CoreSpark.hashed_dataframe(filePopulation)

    assert(actual == 641080076)
  }
}
