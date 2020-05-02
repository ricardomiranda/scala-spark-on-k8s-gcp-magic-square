package com.ricardomiranda.magicsquare

import com.typesafe.scalalogging.StrictLogging

case class Chromosome(value: Seq[Long]) extends StrictLogging {

  /**
    * Change gene in a specific position
    * This function only works if pos is less or equal to the Chromosome's size
    *
    * @param pos  Position to alter
    * @param gene New gene value
    * @return Modified Chromosome
    */
  def modify(gene: Long, pos: Int): Chromosome = pos match {
    case pos if 0 to (this.value.size - 1) contains pos =>
      logger.debug(s"Chromosome modifying gene: ${gene} in position: ${pos}")
      this.copy(value = this.value.updated(pos, gene))
    case _ => this
  }
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
    case xs  => Some(new Chromosome(value))
  }
}
