package com.ricardomiranda.magicsquare

import scala.util.Random
import com.typesafe.scalalogging.StrictLogging

case class Individual(chromosome: Chromosome, fitness: Long)

object Individual extends StrictLogging {

  /**
    * Individual's constructor providing the Chromosome
    *
    * @param chromosome Chromosome
    * @return Option[Individual]
    */
  def apply(chromosome: Chromosome): Option[Individual] =
    MagicSquare.squareDiferences(chromosome = chromosome) match {
      case None => None
      case Some(f) =>
        logger.debug(
          s"Creating individual with Chromosome: ${chromosome} and fitness: ${f}"
        )
        Some(new Individual(chromosome = chromosome, fitness = f))
    }

  /**
    * Individual's constructor for initial porpulation
    *
    * @param chromosomeSize  Chromosome size
    * @param randomGenerator Random generator
    * @return Option[Individual]
    */
  def apply(
      chromosomeSize: Long,
      randomGenerator: Random
  ): Option[Individual] =
    Chromosome(randomGenerator.shuffle(Seq.range(1, chromosomeSize + 1))) match {
      case None =>
        logger.debug(
          s"It was not possible to create individual with size: ${chromosomeSize}"
        )
        None
      case Some(c) =>
        MagicSquare.squareDiferences(chromosome = c) match {
          case None =>
            logger.debug(
              s"It was not possible to create individual with Chromosome: ${c}"
            )
            None
          case Some(f) =>
            logger.debug(
              s"Creating individual with Chromosome: ${c} and fitness: ${f}"
            )
            Some(new Individual(chromosome = c, fitness = f))
        }
    }

  /**
    * With the magic square problem, both the genes and the order of the genes
    * in the chromosome are very important. In fact, for the magic square
    * problem we shouldn't ever have more than one copy of a specific gene in our
    * chromosome. This is because it would create an invalid solution because a number
    * can not be in the solution more than once.
    * Because of this, it's essential that we find and apply a crossover
    * method that produces valid results for our problem.
    * We also need to be respectful of the ordering of the parent's chromosomes
    * during the crossover process. This is because the order of the chromosome
    * the solution's fitness. In fact, it is only the order that matters.
    * Here we will aply ordered crossover.
    *
    * @param crossoverRate   crossover rate
    * @param other           second parent
    * @param randomGenerator Random generator
    * @return New Individual
    */
  val crossover: Double => Random => Seq[Long] => Seq[Long] => Seq[Long] =
    crossoverRate =>
      randomGenerator =>
        me =>
          other =>
            crossoverRate match {
              case crossoverRate
                  if randomGenerator.nextDouble() > crossoverRate =>
                me
              case _ =>
                val rs: scala.collection.immutable.IndexedSeq[Int] =
                  for (i <- 1 to 2)
                    yield randomGenerator.nextInt(
                      me.size - 1
                    )
                val pos1: Int = rs.min
                val pos2: Int = rs.max

                val fstParentContrib: Seq[Long] =
                  me.drop(pos1).take(pos2)
                val sndParentContrib: Seq[Long] =
                  for (i <- other if !fstParentContrib.contains(i)) yield i

                Chromosome(
                  sndParentContrib.take(pos1) ++
                    fstParentContrib ++
                    sndParentContrib.drop(pos1)
                ).get.value
            }

  /**Swap mutation, is an algorithm that will simply swap the genetic information at
    * two points. Swap mutation works by looping though the genes in the individual’s
    * chromosome with each gene being considered for mutation determined by the
    * mutation rate. If a gene is selected for mutation, another random gene in the
    * chromosome is picked and then their positions are swapped.
    *
    * @param mutationRate    mutation rate
    * @param randomGenerator Random generator
    * @return New Individual
    */
  val mutation: Double => Random => Seq[Long] => Seq[Long] =
    mutationRate =>
      randomGenerator =>
        me =>
          mutationRate match {
            case m if randomGenerator.nextDouble() > m => me
            case _ =>
              val pos1: Int =
                randomGenerator.nextInt(me.size - 1)
              val pos2: Int =
                randomGenerator.nextInt(me.size - 1)

              // swaping genes
              Chromosome(me).get
                .modify(gene = me(pos2), pos = pos1)
                .modify(gene = me(pos1), pos = pos2)
                .value
          }
}
