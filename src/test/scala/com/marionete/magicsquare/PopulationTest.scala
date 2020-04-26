package com.marionete.magicsquare

import org.scalatest._
import scala.util.Random
import org.apache.spark.sql.SparkSession

class PopulationTest extends WordSpec with Matchers {
  "A new Population" when {
    "of size 1 with Individual chromosome size 1" should {
      "the chromosome of the lonesome individual == Seq(0)" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

	spark.sparkContext.setLogLevel("WARN")
        
        Population(1,1,new Random(),spark).individuals.first._2.chromosome shouldBe Seq(0)

	spark.stop()
      }
    }

    "of size 10 with Individual chromosome size 1" should {
      "the chromosome of the 5th individual == Seq(0)" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(10,1,new Random(),spark).individuals.zipWithIndex().filter(_._2 == 4).first._1._2.chromosome shouldBe Seq(0)

	spark.stop()
      }
    }

    "of size 5 with Individual chromosome size 3 and Random seed 0" should {
      "the chromosome of the first element of the Population == Seq(2,1,0)" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(5,3,new Random(0),spark).individuals.first._2.chromosome should contain theSameElementsInOrderAs(Vector(2,1,0))

	spark.stop()
      }
    }

    "of size 5" should {
      "be a Population of size 5" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(5,3,new Random(0),spark).individuals.count shouldBe 5

	spark.stop()
      }
    }

    "of size 1 with Individual with chromosome Seq(0,1,5)" should {
      "the chromosome of the element of the Population == Seq(0,1,5)" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(spark.sparkContext.parallelize(Seq((-1,Individual(Seq(0,1,5))))),spark).individuals.first._2.chromosome should contain theSameElementsInOrderAs(Vector(0,1,5))

	spark.stop()
      }
    }

    "of size 2 with Individual with chromosomes Seq(0,1,5)" should {
      "the chromosome of the 2nd element of the Population == Seq(0,1,5)" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(spark.sparkContext.parallelize(
                      Seq((-1,Individual(Seq(0,1,5))),
                          (-1,Individual(Seq(0,1,5))))),
          spark).individuals.first._2.chromosome should contain theSameElementsInOrderAs(Vector(0,1,5))

	spark.stop()
      }
    }
  }

  "A Population" when {
    "of size 1" should {
      "with chromosome Seq(1,2,3,4) have fitness Some(33)" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(spark.sparkContext.parallelize(
                      Seq((-1,Individual(Seq(1,2,3,4))))),
          spark).calcFitness.individuals.first._1 shouldBe 33

	spark.stop()
      }

      "with chromosome Seq(1,2,3,4) retain same order" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(spark.sparkContext.parallelize(
                      Seq((-1,Individual(Seq(1,2,3,4))))),
          spark).calcFitness.individuals.sortByKey().first._2.chromosome should contain theSameElementsInOrderAs(Vector(1,2,3,4))

	spark.stop()
      }
    }
    
    "of size 2, sorted" should {
      "with chromosomes A = Seq(1,2,3,4) and B = Seq(1,2,3,4,5,6,7,8,9), B be 2nd" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(spark.sparkContext.parallelize(
                      Seq((-1,Individual(Seq(1,2,3,4))),
                          (-1,Individual(Seq(1,2,3,4,5,6,7,8,9))))),
          spark).calcFitness.individuals.sortByKey().zipWithIndex().filter(_._2 == 1).first._1._2.chromosome should contain theSameElementsInOrderAs(Vector(1,2,3,4,5,6,7,8,9))

	spark.stop()
      }
    }

    "of size 3, chromosomes A = Seq(1,2,3,4), B = Seq(1,2,3,4,5,6,7,8,9) and C = Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)" should {
      "with tournament size 2, return A" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(spark.sparkContext.parallelize(
                      Seq((-1,Individual(Seq(1,2,3,4))),
                          (-1,Individual(Seq(1,2,3,4,5,6,7,8,9))),
                          (-1,Individual(Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16))))),
          spark).calcFitness.tournamentSelection(2, new Random(2)).chromosome should contain theSameElementsInOrderAs(Vector(1,2,3,4,5,6,7,8,9))

	spark.stop()
      }
    }
    
    "of size 3 with tournament size 2 and number of children 2" should {
      "return a sequence of parents with size 2" in {
        val spark: SparkSession = SparkSession.builder()
	  .appName("Testing Population Magic Squares wiht Spark")
	  .master("local[*]")
	  .getOrCreate()

        Population(spark.sparkContext.parallelize(
                      Seq((-1,Individual(Seq(1,2,3,4))),
                          (-1,Individual(Seq(1,2,3,4,5,6,7,8,9))),
                          (-1,Individual(Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16))))),
          spark).calcFitness.selectParents(2,2).count shouldBe 2

	spark.stop()
      }
    }
    
    "of size 3" should {
      "return a Population of size 3" in {
        val spark: SparkSession = SparkSession.builder()
          .appName("Testing Population Magic Squares wiht Spark")
          .master("local[*]")
          .getOrCreate()

        Population(spark.sparkContext.parallelize(
                      Seq((-1,Individual(Seq(1,2,3,4))),
                          (-1,Individual(Seq(1,2,3,4))),
                          (-1,Individual(Seq(1,2,3,4))))),
          spark).calcFitness.newGeneration(3,2,0.5,0.5).individuals.count shouldBe 3

	spark.stop()
      }
    }
  }
    
  "A list of parents" when {
    "of size 1 with crossover = 0.5 and mutation = 0.5" should {
      "return a Population of size 1" in {
        val spark: SparkSession = SparkSession.builder()
          .appName("Testing Population Magic Squares wiht Spark")
          .master("local[*]")
          .getOrCreate()

        Population(spark.sparkContext.parallelize(Seq((-1,Individual(Seq(1,2,3,4))))), spark)
          .offspring(spark.sparkContext.parallelize(Seq((Individual(Seq(1,2,3,4)), Individual(Seq(1,2,3,4))))), 0.5, 0.5)
          .count shouldBe 1

	spark.stop()
      }
    }
  }
}
