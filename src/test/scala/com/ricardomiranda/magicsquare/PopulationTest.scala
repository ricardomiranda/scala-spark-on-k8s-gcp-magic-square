package com.ricardomiranda.magicsquare

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.scalatest._

import scala.util.Random
import shapeless.Data
import checkers.units.quals.s

class PopulationTest extends funsuite.AnyFunSuite with DataFrameSuiteBase {
  test(testName = "empty dataframe") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 0,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    assert(p.individuals.count() == 0)
  }

  test(testName = "size one dataframe") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 1,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    assert(p.individuals.count() == 1)
  }

  test(testName = "Check contents of population DF") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 10,
        randomGenerator = new Random(0),
        sparkSession = spark
      )
    val actual: Int = CoreSpark.hashed_dataframe(p.individuals)

    assert(actual == 205707748)
  }

  test(testName = "Fitness of Population with size 0") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 0,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[Double] = p.populationFitness(percentile = 0.10)
    assert(expected == None)
  }

  test(testName = "Fitness of Population with size 1") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 1,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[Double] = p.populationFitness(percentile = 0.10)
    assert(expected == Some(25.0))
  }

  test(testName = "Fitness of Population with size 10 using percentile 0.05") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 10,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[Double] = p.populationFitness(percentile = 0.05)
    assert(expected == Some(22.0))
  }

  test(testName = "Fitness of Population with size 10 using percentile 0.10") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 10,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[Double] = p.populationFitness(percentile = 0.10)
    assert(expected == Some(23.5))
  }

  test(testName = "Tournament selection from Population with size 0") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 0,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Option[DataFrame] =
      p.tournamentSelection(
        nbrOfParents = 2,
        randomGenerator = new Random(0),
        tournamentSize = 1
      )
    assert(actual == None)
  }

  test(testName = "Tournament selection from Population with size 1") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 1,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Option[DataFrame] =
      p.tournamentSelection(
        nbrOfParents = 2,
        randomGenerator = new Random(0),
        tournamentSize = 1
      )
    assert(actual == None)
  }

  test(testName =
    "Tournament selection of 2 parents from Population with size 2"
  ) {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 2,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Option[DataFrame] =
      p.tournamentSelection(
        nbrOfParents = 2,
        randomGenerator = new Random(0),
        tournamentSize = 1
      )

    val expected: Some[DataFrame] =
      Some(
        spark
          .createDataFrame(
            Seq(
              (Chromosome(Seq(2, 1, 4, 3)).get.value, 25),
              (Chromosome(Seq(2, 1, 4, 3)).get.value, 25)
            )
          )
          .toDF("chromosome", "fitness")
      )

    actual.get.show()
    assert(
      CoreSpark.hashed_dataframe(actual.get) == CoreSpark.hashed_dataframe(
        expected.get
      )
    )
    actual.get.show()
  }

  test(testName =
    "Tournament selection with size 5 of 10 parents from of Population with size 100"
  ) {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 100,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Option[DataFrame] =
      p.tournamentSelection(
        nbrOfParents = 10,
        randomGenerator = new Random(0),
        tournamentSize = 5
      )

    assert(CoreSpark.hashed_dataframe(actual.get) == 1254853226)
  }

  test(testName = "Fitest individual of Population with size 0") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 0,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[Long] = p.fitestIndividual
    assert(expected == None)
  }

  test(testName = "Fitest individual of Population with size 1") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 1,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[Long] = p.fitestIndividual
    assert(expected == Some(25))
  }

  test(testName = "Fitest individual of Population with size 100") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 100,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[Long] = p.fitestIndividual
    assert(expected == Some(22))
  }

  test(testName = "Select parents form Population with size 0") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 0,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[DataFrame] =
      p.selectParents(
        nbrOfOffspring = 2,
        randomGenerator = new Random(0),
        tournamentSize = 2
      )
    assert(expected == None)
  }

  test(testName = "Select parents form Population with size 1") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 1,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[DataFrame] =
      p.selectParents(
        nbrOfOffspring = 2,
        randomGenerator = new Random(0),
        tournamentSize = 2
      )
    assert(expected == None)
  }

  test(testName = "Select parents for 1 offspring form Population with size 2") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 2,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[DataFrame] =
      p.selectParents(
        nbrOfOffspring = 2,
        randomGenerator = new Random(0),
        tournamentSize = 2
      )

    assert(expected.get.count == 2)
  }

  test(testName =
    "Select parents for 5 offspring form Population with size 2000"
  ) {
    val p: Population =
      Population(
        chromosomeSize = 9,
        populationSize = 20000,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Option[DataFrame] =
      p.selectParents(
        nbrOfOffspring = 5,
        randomGenerator = new Random(0),
        tournamentSize = 5
      )

    assert(expected.get.count == 5)
  }
}
