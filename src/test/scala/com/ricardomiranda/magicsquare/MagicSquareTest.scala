package com.ricardomiranda.magicsquare

import org.scalatest._
import breeze.linalg._

class MagicSquareTest
    extends wordspec.AnyWordSpec
    with matchers.should.Matchers {
  "A DenseMatrix" when {
    "chromosome Seq(1)" should {
      "be matrix == ((1))" in {
        val c: Option[Chromosome] = Chromosome(Seq(1L))
        MagicSquare.matrix(c.get) shouldBe Some(DenseMatrix((1)))
      }
    }

    "chromosome Seq(1,2,3,4)" should {
      "be matrix == ((1,2),(3,4))" in {
        val c: Option[Chromosome] = Chromosome(Seq(1L, 2L, 3L, 4L))
        MagicSquare.matrix(c.get) shouldBe Some(DenseMatrix((1, 2), (3, 4)))
      }
    }

    "chromosome Seq(1,2,3,4,5,6,7,8,9)" should {
      "be matrix == ((1,2,3),(4,5,6),(7,8,9))" in {
        val c: Option[Chromosome] =
          Chromosome(Seq(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L))
        MagicSquare.matrix(c.get) shouldBe Some(
          DenseMatrix(
            (1, 2, 3),
            (4, 5, 6),
            (7, 8, 9)
          )
        )
      }
    }
  }

  "Calculating the fitness of a MagicSquare" when {
    "chromosome Seq(1)" should {
      "be fitness == 0" in {
        val c: Option[Chromosome] = Chromosome(Seq(1L))
        MagicSquare.squareDiferences(chromosome = c.get) shouldBe Some(0)
      }
    }

    "chromosome Seq(1,2,3,4)" should {
      "be fitness == 0" in {
        val c: Option[Chromosome] = Chromosome(Seq(1L, 2L, 3L, 4L))
        MagicSquare.squareDiferences(chromosome = c.get) shouldBe Some(33)
      }
    }

    "chromosome Seq(1,1,1,1)" should {
      "be fitness == 0" in {
        val c: Option[Chromosome] = Chromosome(Seq(1L, 1L, 1L, 1L))
        MagicSquare.squareDiferences(chromosome = c.get) shouldBe Some(0)
      }
    }
  }
}
