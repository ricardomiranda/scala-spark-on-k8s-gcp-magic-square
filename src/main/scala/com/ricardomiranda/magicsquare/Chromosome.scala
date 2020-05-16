package com.ricardomiranda.magicsquare

import com.typesafe.scalalogging.StrictLogging

class Chromosome(value: Seq[Long]) extends StrictLogging {

  val this.value: Seq[Long] = value

  /**
    * Change gene in a specific position
    * This function only works if pos is less or equal to the Chromosome's size
    *
    * @param pos  Position to alter
    * @param gene New gene value
    * @return Modified Chromosome
    */
  def modify(gene: Long, pos: Int): Chromosome = pos match {
    case x: Int if 0 to (this.value.size - 1) contains x =>
      logger.debug(s"Chromosome modifying gene: ${gene} in position: ${x}")
      new Chromosome(value = this.value.updated(x, gene))
    case _ => this
  }

  /**
    * Retrive the chromosome value
    *
    * @return The chromosome
    */
  def value(): Seq[Long] = this.value
}

object Chromosome {

  /**
    * Constuctor for Chromosome, returns an Option
    *
    * @param value The Chromosome sequence of genes
    * @return Option[Chromosome]
    */
  def apply(value: Seq[Long]): Option[Chromosome] = value match {
    case Nil => None
    case xs: Seq[Long] => Some(new Chromosome(xs))
  }
}
