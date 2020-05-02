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

    // "chromosome is given" should {
    //   "be Individual.chromosome == Seq(0,1,2,3,4,5)" in {
    //     Individual(Seq(0, 1, 2, 3, 4, 5)).chromosome shouldBe Seq(0, 1, 2, 3, 4,
    //       5)
    //   }
    // }

    //   "modify chromosome Seq(0,1,2) from 1 -> 9" should {
    //     "be Seq(0,9,2)" in {
    //       Individual(Seq(0,1,2)).modify(pos = 1, gene = 9).chromosome shouldBe Seq(0,9,2)
    //     }
    //   }
    // }

    // "An existing Individal " when {
    //   "whith chromosome Seq(1,2,3,4)" should {
    //     "be Individual.fitness == Some(33)" in {
    //       Individual(Seq(1,2,3,4)).calcFitness shouldBe 33
    //     }
    //   }

    //   "whith chromosome Seq(1,2,3,4,5,6,7,8,9,10) and mutationRate = 0.0" should {
    //     "have no mutation" in {
    //       Individual(Seq(1,2,3,4,5,6,7,8,9,10)).mutation(0.0,new Random(0)).chromosome should contain theSameElementsInOrderAs(Vector(1,2,3,4,5,6,7,8,9,10))
    //     }
    //   }

    //   "whith chromosome Seq(1,2,3,4,5,6,7,8,9,10) and mutationRate = 1.0" should {
    //     "have mutation form Seq(1,2,3,4,5,6,7,8,9,10) to Seq(6,10,8,3,4,9,2,7,1,5)" in {
    //       Individual(Seq(1,2,3,4,5,6,7,8,9,10)).mutation(1.0,new Random(0)).chromosome should contain theSameElementsInOrderAs(Vector(1,2,5,4,3,6,7,8,9,10))
    //     }
    //   }
    // }

    // "Two existing Individals" when {
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
