package com.ricardomiranda.magicsquare

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import org.apache.spark.sql.types._
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

  test(testName = "Tournament selection from of Population with size 0") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 0,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Option[Individual] = p.tournamentSelection(tournamentSize = 1)
    assert(actual == None)
  }

  test(testName = "Tournament selection from of Population with size 1") {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 1,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Option[Individual] = p.tournamentSelection(tournamentSize = 1)
    val expected: Some[Individual] =
      Some(
        Individual(chromosome = Chromosome(Seq(4, 1, 2, 3)).get, fitness = 25)
      )
    assert(actual == expected)
  }

  test(testName =
    """
  Tournament selection with size 5 from of Population with size 100"""
  ) {
    val p: Population =
      Population(
        chromosomeSize = 4,
        populationSize = 100,
        randomGenerator = new Random(0),
        sparkSession = spark
      )

    val actual: Long =
      p.tournamentSelection(seed = 0, tournamentSize = 5).get.fitness
    val expected: Long =
      Some(
        Individual(chromosome = Chromosome(Seq(2, 4, 3, 1)).get, fitness = 22)
      ).get.fitness

    assert(actual == expected)
  }

  //   "of size 2, sorted" should {
  //     "with chromosomes A = Seq(1,2,3,4) and B = Seq(1,2,3,4,5,6,7,8,9), B be 2nd" in {
  //       val spark: SparkSession = SparkSession.builder()
  //   .appName("Testing Population Magic Squares wiht Spark")
  //   .master("local[*]")
  //   .getOrCreate()

  //       Population(spark.sparkContext.parallelize(
  //                     Seq((-1,Individual(Seq(1,2,3,4))),
  //                         (-1,Individual(Seq(1,2,3,4,5,6,7,8,9))))),
  //         spark).calcFitness.individuals.sortByKey().zipWithIndex().filter(_._2 == 1).first._1._2.chromosome should contain theSameElementsInOrderAs(Vector(1,2,3,4,5,6,7,8,9))

  // spark.stop()
  //     }
  //   }

  //   "of size 3, chromosomes A = Seq(1,2,3,4), B = Seq(1,2,3,4,5,6,7,8,9) and C = Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)" should {
  //     "with tournament size 2, return A" in {
  //       val spark: SparkSession = SparkSession.builder()
  //   .appName("Testing Population Magic Squares wiht Spark")
  //   .master("local[*]")
  //   .getOrCreate()
  //   "of size 3 with tournament size 2 and number of children 2" should {
  //     "return a sequence of parents with size 2" in {
  //       val spark: SparkSession = SparkSession.builder()
  //   .appName("Testing Population Magic Squares wiht Spark")
  //   .master("local[*]")
  //   .getOrCreate()

  //       Population(spark.sparkContext.parallelize(
  //                     Seq((-1,Individual(Seq(1,2,3,4))),
  //                         (-1,Individual(Seq(1,2,3,4,5,6,7,8,9))),
  //                         (-1,Individual(Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16))))),
  //         spark).calcFitness.selectParents(2,2).count shouldBe 2

  // spark.stop()
  //     }
  //   }

  //   "of size 3" should {
  //     "return a Population of size 3" in {
  //       val spark: SparkSession = SparkSession.builder()
  //         .appName("Testing Population Magic Squares wiht Spark")
  //         .master("local[*]")
  //         .getOrCreate()

  //       Population(spark.sparkContext.parallelize(
  //                     Seq((-1,Individual(Seq(1,2,3,4))),
  //                         (-1,Individual(Seq(1,2,3,4))),
  //                         (-1,Individual(Seq(1,2,3,4))))),
  //         spark).calcFitness.newGeneration(3,2,0.5,0.5).individuals.count shouldBe 3

  // spark.stop()
  //     }
  //   }
  // }

  // "A list of parents" when {
  //   "of size 1 with crossover = 0.5 and mutation = 0.5" should {
  //     "return a Population of size 1" in {
  //       val spark: SparkSession = SparkSession.builder()
  //         .appName("Testing Population Magic Squares wiht Spark")
  //         .master("local[*]")
  //         .getOrCreate()

  //       Population(spark.sparkContext.parallelize(Seq((-1,Individual(Seq(1,2,3,4))))), spark)
  //         .offspring(spark.sparkContext.parallelize(Seq((Individual(Seq(1,2,3,4)), Individual(Seq(1,2,3,4))))), 0.5, 0.5)
  //         .count shouldBe 1

  // spark.stop()
  //     }
  //   }
}
