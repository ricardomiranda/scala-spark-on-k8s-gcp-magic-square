package com.marionete.magicsquare

import scala.util.Random
import scala.math.Ordered.orderingToOrdered

case class Individual (chromosome: Seq[Long]) {
  def modify(pos: Int, gene: Long) = 
    Individual(this.chromosome.updated(pos, gene))

  def calcFitness: Int = 
    MagicSquare(this.chromosome).squareDiferences.toInt

/**Swap mutation, is an algorithm that will simply swap the genetic information at
 * two points. Swap mutation works by looping though the genes in the individual’s
 * chromosome with each gene being considered for mutation determined by the
 * mutation rate. If a gene is selected for mutation, another random gene in the
 * chromosome is picked and then their positions are swapped.
 */
  def mutation(m: Double, r: Random): Individual = m match {
  // m -> mutation rate
    case m if r.nextDouble() > m => this
    case _ => 
      val pos1 = r.nextInt(this.chromosome.size-1)
      val pos2 = r.nextInt(this.chromosome.size-1)

      // swaping genes
      this.modify(pos1, this.chromosome(pos2)).modify(pos2, this.chromosome(pos1))
  }

/**With the magic square problem, both the genes and the order of the genes 
 * in the chromosome are very important. In fact, for the magic square
 * problem we shouldn't ever have more than one copy of a specific gene in our
 * chromosome. This is because it would create an invalid solution because a number
 * can not be in the solution more than once. Consider a case where we
 * Because of this, it's essential that we find and apply a crossover
 * method that produces valid results for our problem.
 * We also need to be respectful of the ordering of the parent's chromosomes
 * during the crossover process. This is because the order of the chromosome
 * the solution's fitness. In fact, it is only the order that matters.
 * Here we will aply ordered crossover.
 */
  def crossover(other: Individual, c: Double, r: Random): Individual = c match {
  // c -> crossover rate, other -> second parent  
    case c if r.nextDouble() < c => this
    case _ =>
      val rs = for (i <- 1 to 2) yield r.nextInt(this.chromosome.size-1)
      val pos1 = rs.min
      val pos2 = rs.max

      // fst parent contribution
      val fstParentContrib = this.chromosome.drop(pos1).take(pos2)
      val sndParentContrib = for (i <- other.chromosome if !fstParentContrib.contains(i)) yield i
      val chromosome = sndParentContrib.take(pos1) ++
                       fstParentContrib ++
		       sndParentContrib.drop(pos1)
      Individual(chromosome)
  }
}

object Individual {
  implicit def orderingByFitness[A <: Individual]: Ordering[A] =
    Ordering.by(_.chromosome.sum)

  def apply(chromosomeSize: Long, r: Random) =
    new Individual(r.shuffle(Seq.range(0, chromosomeSize)))
}
