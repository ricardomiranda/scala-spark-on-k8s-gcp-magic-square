package com.ricardomiranda.magicsquare

import breeze.linalg._
import breeze.numerics._

import scala.annotation.tailrec
import scala.util.Try

object MagicSquare {

  /**
    * This computation is the fitness of a Individual
    *
    * @param chromosome A Chromosome
    * @return Option[Fitness]
    */
  def squareDiferences(chromosome: Chromosome): Option[Long] =
    MagicSquare.matrix(chromosome) match {

      case Some(matrix) =>
        Try {
          val sqr: Long => Long = x => x * x

          def lines: Seq[Long] = {
            Seq(trace(matrix), trace(matrix(*, ::).map(reverse(_)))) ++ (sum(
              matrix(*, ::)
            )).toArray.toSeq ++ (sum(matrix(::, *))).t.toArray.toSeq
          }

          @tailrec
          def loop(xs: Seq[Long], acc: Long): Long = xs match {
            case Seq(x) => acc
            case x :: y :: xs => loop(y :: xs, acc + sqr(x - y))
          }

          loop(lines, 0)
        }.toOption
      case None => None
    }

  /**
    * Returns a denseMatrix from a Chromosome, requires a Chromosome with a
    * size suitable to be turned into a square
    *
    * @param chromosome A Chromosome
    * @return Option[DenseMatrix]
    */
  def matrix(chromosome: Chromosome): Option[DenseMatrix[Long]] = {
    Try {
      val n: Int = sqrt(chromosome.value.size).toInt
      DenseMatrix(chromosome.value.grouped(n).toSeq: _*)
    }.toOption
  }
}
