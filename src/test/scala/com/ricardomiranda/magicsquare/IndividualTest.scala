package com.ricardomiranda.magicsquare

import org.scalatest._
import scala.util.Random

class IndividualTest
    extends wordspec.AnyWordSpec
    with matchers.should.Matchers {

  "A new Individal " when {
    "chromosome size is 1" should {
      "be Individual.chromosome == Seq(0)" in {
        val actual: Option[Individual] = Individual(
          chromosomeSize = 1,
          randomGenerator = new Random(0L)
        )

        val expected: Option[Chromosome] = Chromosome(Seq(0))

        actual.get.chromosome shouldBe expected.get
      }
    }

    "chromosome size is 2 and Random seed 0" should {
      "be Individual == None" in {
        val actual: Option[Individual] = Individual(
          chromosomeSize = 2,
          randomGenerator = new Random(0L)
        )

        actual shouldBe None
      }
    }

    "chromosome size is 3 and Random seed 0" should {
      "be Individual == None" in {
        val actual: Option[Individual] = Individual(
          chromosomeSize = 3,
          randomGenerator = new Random(0L)
        )

        actual shouldBe None
      }
    }

    "chromosome size is 4 and Random seed 0" should {
      "be Individual.chromosome == Seq(3, 0, 1, 2)" in {
        val actual: Option[Individual] = Individual(
          chromosomeSize = 4,
          randomGenerator = new Random(0L)
        )

        val expected: Option[Chromosome] = Chromosome(Seq(3, 0, 1, 2))

        actual.get.chromosome shouldBe expected.get
      }
    }

    "chromosome size is 9 and Random seed 0" should {
      "be Individual.chromosome == Seq(3, 7, 2, 1, 0, 5, 4, 8, 6)" in {
        val actual: Option[Individual] = Individual(
          chromosomeSize = 9,
          randomGenerator = new Random(0L)
        )

        val expected: Option[Chromosome] =
          Chromosome(Seq(3, 7, 2, 1, 0, 5, 4, 8, 6))

        actual.get.chromosome shouldBe expected.get
      }
    }

    "chromosome is given" should {
      "be Individual.fitness == Seq(3, 7, 2, 1, 0, 5, 4, 8, 6)" in {
        val c: Chromosome = Chromosome(Seq(3, 7, 2, 1, 0, 5, 4, 8, 6)).get

        val actual: Option[Individual] = Individual(chromosome = c)

        val expected: Option[Long] =
          MagicSquare.squareDiferences(chromosome = c)

        actual.get.fitness shouldBe expected.get
      }
    }
  }

  "An existing Individal " when {
    "whith chromosome Seq(1,2,3,4)" should {
      "be Individual.fitness == Some(33)" in {
        val c: Chromosome = Chromosome(Seq(1, 2, 3, 4)).get

        val actual: Option[Individual] = Individual(chromosome = c)

        actual.get.fitness shouldBe 33
      }
    }

    "whith chromosome Seq(1,2,3,4,5,6,7,8,9) and mutationRate = 0.0" should {
      "have no mutation" in {
        val c: Chromosome = Chromosome(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)).get

        val actual: Individual = Individual(chromosome = c).get
          .mutation(mutationRate = 0.0, randomGenerator = new Random(0L))

        actual.chromosome.value should contain theSameElementsInOrderAs (
            Vector(1, 2, 3, 4, 5, 6, 7, 8, 9))
      }
    }

      "whith chromosome Seq(1,2,3,4,5,6,7,8,9) and mutationRate = 1.0" should {
        "have mutation form Seq(1,2,3,4,5,6,7,8,9) to Seq(6,8,3,4,9,2,7,1,5)" in {
        val c: Chromosome = Chromosome(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)).get

        val actual: Individual = Individual(chromosome = c).get
          .mutation(mutationRate = 1.0, randomGenerator = new Random(0L))

        actual.chromosome.value should contain theSameElementsInOrderAs (
            Vector(1, 5, 3, 4, 2, 6, 7, 8, 9))
        }
      }
    }

    "Two existing Individals" when {
    //   "whith chromosomes Seq(1,2,3,4) and Seq(1,2,3,4) and crossover = 0.0" should {
    //     "produce Seq(1,2,3,4)" in {
    //       val actual: Seq[Long] =
    //         Individual(Seq(1,2,3,4))
    //           .crossover(Individual(Seq(1,2,3,4)), 0.0, new Random(0))
    //           .chromosome
    //       actual should contain theSameElementsAs(Vector(1,2,3,4))
    //     }
    //   }

    //   "whith chromosomes Seq(1,2,3,4) and Seq(1,2,3,4) and crossover 1.0" should {
    //     "produce Seq(1,2,3,4)" in {
    //       val actual: Seq[Long] =
    //         Individual(Seq(1,2,3,4))
    //           .crossover(Individual(Seq(1,2,3,4)), 1.0, new Random(0))
    //           .chromosome
    //       actual should contain theSameElementsInOrderAs(Vector(1,2,3,4))
    //     }
    //   }

    //   "whith chromosomes Seq(1,2,3,4) and Seq(4,3,2,1) and crossover 1.0" should {
    //     "produce Seq(1,2,3,4)" in {
    //       val actual: Seq[Long] =
    //         Individual(Seq(1,2,3,4))
    //           .crossover(Individual(Seq(4,3,2,1)), 1.0, new Random(0))
    //           .chromosome
    //       actual should contain theSameElementsInOrderAs(Vector(1,2,3,4))
    //     }
    //   }
  }
}
