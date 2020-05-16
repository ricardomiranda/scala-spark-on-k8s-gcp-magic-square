package com.ricardomiranda.magicsquare

import org.scalatest._

class ChromosomeTest
  extends wordspec.AnyWordSpec
    with matchers.should.Matchers {
  "A Chromosome" when {
    "Constructed with Seq()" should {
      "be with None" in {
        val xs: Seq[Long] = Seq[Long]()
        Chromosome(xs) shouldBe None
      }
    }

    "Constructed with Seq(1)" should {
      "be with value == (1)" in {
        val xs: Seq[Long] = Seq(1L)
        Chromosome(xs).get.value shouldBe xs
      }
    }

    "Constructed with Seq(1, 2, 3, 4)" should {
      "be with value == (1, 2, 3, 4)" in {
        val xs: Seq[Long] = Seq(1L, 2L, 3L, 4L)
        Chromosome(xs).get.value shouldBe xs
      }
    }
  }

  "Modifying a Chromosome" should {
    "change nothing if the position is out of bounds, 1" in {
      val xs: Seq[Long] = Seq(1L)
      val c: Option[Chromosome] = Chromosome(xs)
      c.get.modify(gene = 0L, pos = 1) shouldBe c.get
    }
  }

  "Modifying a Chromosome" should {
    "change nothing if the position is out of bounds, 2" in {
      val xs: Seq[Long] = Seq(1L)
      val c: Option[Chromosome] = Chromosome(xs)
      c.get.modify(gene = 0L, pos = 100) shouldBe c.get
    }
  }

  "Modifying a Chromosome" should {
    "change nothing if the position is negative, 1" in {
      val xs: Seq[Long] = Seq(1L)
      val c: Option[Chromosome] = Chromosome(xs)
      c.get.modify(gene = 0L, pos = -1) shouldBe c.get
    }
  }

  "Modifying a Chromosome" should {
    "change nothing if the position is negative, 2" in {
      val xs: Seq[Long] = Seq(1L)
      val c: Option[Chromosome] = Chromosome(xs)
      c.get.modify(gene = 0L, pos = -100) shouldBe c.get
    }
  }

  "Modifying a Chromosome" should {
    "change a gene when given a valid input" in {
      val xs: Seq[Long] = Seq(1L)
      val c: Option[Chromosome] = Chromosome(xs)
      c.get.modify(gene = 0L, pos = 0).value() shouldBe Chromosome(Seq(0L)).get.value()
    }
  }
}
