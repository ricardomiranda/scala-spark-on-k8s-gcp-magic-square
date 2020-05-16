package com.ricardomiranda.magicsquare

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest._

import scala.util.Random

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

    assert(
      CoreSpark.hashed_dataframe(actual.get) == CoreSpark.hashed_dataframe(
        expected.get
      )
    )
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

    val expected: None.type = p.fitestIndividual match {
      case None => None
    }
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

    val expected: (Seq[Long], Long) = p.fitestIndividual match {
      case Some((c, f)) => (c, f)
    }
    assert(expected == (Seq(4, 1, 2, 3), 25))
  }

  test(testName = "Fitest individual of Population with size 100") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 100,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val expected: Long = p.fitestIndividual match {
      case Some((_, f: Long)) => f
    }
    assert(expected == 22)
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

  test(testName = "Offspring from population of 2") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 2,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val parents: DataFrame =
      p.selectParents(
        nbrOfOffspring = 2,
        randomGenerator = new Random(0),
        tournamentSize = 2
      )
        .get

    val actual: DataFrame = p.offspring(
      crossoverRate = 1.0,
      mutationRate = 1.0,
      parents = parents,
      randomGenerator = new Random(0)
    )

    assert(CoreSpark.hashed_dataframe(actual) == -1544732213)
  }

  test(testName = "190 offspring from population of 200") {
    val p: Population =
      Population(
        chromosomeSize = 9,
        populationSize = 200,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val parents: DataFrame =
      p.selectParents(
        nbrOfOffspring = 190,
        randomGenerator = new Random(0),
        tournamentSize = 5
      )
        .get

    val actual: DataFrame = p.offspring(
      crossoverRate = 0.90,
      mutationRate = 0.05,
      parents = parents,
      randomGenerator = new Random(0)
    )

    assert(
      CoreSpark
        .hashed_dataframe(actual.orderBy("fitness", "chromosome")) == -589824148
    )
  }

  test(testName = "Generate a new generation of 2 with 0 offspring") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 2,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Population = p.newGeneration(
      crossoverRate = 0.0,
      elite = 2,
      mutationRate = 0.05,
      randomGenerator = new Random(0),
      tournamentSize = 2
    )

    assert(
      CoreSpark.hashed_dataframe(
        p.individuals.orderBy("fitness", "chromosome")
      ) == CoreSpark
        .hashed_dataframe(
          actual.individuals.orderBy("fitness", "chromosome")
        )
    )
  }

  test(testName = "Generate a new generation of 4 with 2 offspring") {
    val p: Population =
      Population(
        chromosomeSize = 9,
        populationSize = 4,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Population = p.newGeneration(
      crossoverRate = 1.0,
      elite = 2,
      mutationRate = 0.05,
      randomGenerator = new Random(0),
      tournamentSize = 2
    )

    assert(actual.individuals.count() == 4)
  }

  test(testName = "Generate a new generation of 200 with 195 offspring") {
    val p: Population =
      Population(
        chromosomeSize = 9,
        populationSize = 200,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Population = p.newGeneration(
      crossoverRate = 1.0,
      elite = 5,
      mutationRate = 0.05,
      randomGenerator = new Random(0),
      tournamentSize = 5
    )

    assert(actual.individuals.count() == 200L)
  }
}
