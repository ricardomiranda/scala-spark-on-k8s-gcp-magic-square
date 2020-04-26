package com.marionete.magicsquare

import org.scalatest.{ WordSpec, Matchers }
import breeze.linalg._

class MagicSquareTest extends WordSpec with Matchers {
  "A new MagicSquare" when {
    "chromosome Seq(1)" should {
      "be m == ((1))" in {
        MagicSquare(Seq(1L)).m shouldBe DenseMatrix((1))
      }
    }

    "chromosome Seq(1,2,3,4)" should {
      "be m == ((1,2),(3,4))" in {
        MagicSquare(Seq(1L,2L,3L,4L)).m shouldBe DenseMatrix((1,2),(3,4))
      }
    }

    "chromosome Seq(1,2,3,4,5,6,7,8,9)" should {
      "be m == ((1,2,3),(4,5,6),(7,8,9))" in {
        MagicSquare(Seq(1L,2L,3L,4L,5L,6L,7L,8L,9L)).m shouldBe DenseMatrix((1,2,3),(4,5,6),(7,8,9))
      }
    }
  }

  "Calculating the fitness of a MagicSquare" when {
    "chromosome Seq(1)" should {
      "be fitness == 0" in {
        MagicSquare(Seq(1L)).squareDiferences shouldBe 0
      }
    }

    "chromosome Seq(1,2,3,4)" should {
      "be fitness == 0" in {
        MagicSquare(Seq(1L,2L,3L,4L)).squareDiferences shouldBe 33
      }
    }

    "chromosome Seq(1,1,1,1)" should {
      "be fitness == 0" in {
        MagicSquare(Seq(1L,1L,1L,1L)).squareDiferences shouldBe 0
      }
    }
  }
}
