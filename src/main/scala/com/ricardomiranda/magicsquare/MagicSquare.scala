package com.ricardomiranda.magicsquare

import breeze.linalg._
import breeze.numerics._

import scala.annotation.tailrec

object MagicSquare {

  /**
    * Returns a denseMatrix from a Chromosome, requires a non zero length
    * Chromosome
    *
    * @param chromosome A Chromosome
    * @return Option[DenseMatrix]
    */
  def matrix(c: Chromosome): Option[DenseMatrix[Long]] = c match {
    case c if c.value != Seq.empty =>
      val n: Int = sqrt(c.value.size).toInt
      Some(DenseMatrix(c.value.grouped(n).toSeq: _*))
    case _ => None
  }

  /**
    * This computation is the fitness of a Individual
    *
    * @return Option[Fitness]
    */
  def squareDiferences(chromosome: Chromosome): Option[Long] =
    MagicSquare.matrix(chromosome) match {

      case Some(matrix) =>
        val sqr: Long => Long = x => x * x

        def lines: Seq[Long] = {
          Seq(trace(matrix), trace(matrix(*, ::).map(reverse(_)))) ++ (sum(
            matrix(*, ::)
          )).toArray.toSeq ++ (sum(matrix(::, *))).t.toArray.toSeq
        }

        @tailrec
        def loop(xs: Seq[Long], acc: Long): Long = xs match {
          case Seq(x)       => acc
          case x :: y :: xs => loop(y :: xs, acc + sqr(x - y))
        }

        Some(loop(lines, 0))

      case None => None
    }
}
