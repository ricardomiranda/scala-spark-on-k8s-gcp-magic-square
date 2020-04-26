package com.marionete.magicsquare

import breeze.linalg._
import breeze.numerics._

case class MagicSquare(m: DenseMatrix[Long]) {
  def squareDiferences: Long = {
    def sqr(x: Long) = x*x

    def lines: List[Long] = {
      List(trace(this.m),
           trace(this.m(*,::).map(reverse(_)))
		  ) ++ (sum(this.m(*,::))).toArray.toList ++ (sum(this.m(::,*))).t.toArray.toList
    }

    import scala.annotation.tailrec
    @tailrec
    def loop(xs: List[Long], acc: Long): Long = xs match {
      case Seq(x) => acc
      case x::y::xs => loop(y::xs, acc + sqr(x-y))
    }

    loop(lines, 0)
  }
}

object MagicSquare {
  def apply(chromosome: Seq[Long]) = {
    val n = sqrt(chromosome.size).toInt
    new MagicSquare(DenseMatrix(chromosome.grouped(n).toSeq:_*))
  }
}
