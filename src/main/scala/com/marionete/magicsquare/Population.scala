package com.marionete.magicsquare

import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

case class Population(individuals: RDD[(Int, Individual)], spark: SparkSession) {
  def calcFitness: Population = 
    this.copy(individuals = this.individuals.map(x => (x._2.calcFitness, x._2)))

  def populationFitness(percentile: Double):  Double = {
    val percentilePop = (percentile*this.individuals.count).toInt
    this.individuals
        .takeOrdered(percentilePop)
	.map{ case (k,_) => k }
        .sum.toDouble / percentilePop.toDouble
  }

/** Tournament selection selects its parents by running a series of "tournaments".
  * First, individuals are randomly selected from the population and entered into a
  * tournament. Next, these individuals can be thought to compete with each other
  * by comparing their fitness values, then choosing the individual with the highest
  * fitness for the parent.
  */
  def tournamentSelection(n: Int, r: Random): Individual =
    (this.individuals).takeSample(withReplacement = false, num = n, seed = r.nextLong).head._2

  def selectParents(n: Long, t: Int): RDD[(Individual, Individual)] =
  // n -> number of children, t -> tournament size
    spark.sparkContext.parallelize((1 to n.toInt).map(_ => (tournamentSelection(t, new Random()), tournamentSelection(t, new Random()))))

  def offspring(parents: RDD[(Individual,Individual)], m: Double, c: Double): RDD[(Int,Individual)] = 
  // parents -> list of parents, m -> mutation rate, c -> crossover rate
    parents.map{ case (p1,p2) => (-1, p1.crossover(p2, c, new Random()).mutation(m, new Random())) }

  def newGeneration(e: Int, t: Int, m: Double, c: Double): Population = {
  // e -> elite, t -> tournament size, m -> mutation rate, c -> crossover rate
    val elitePopulation: RDD[(Int,Individual)] =
      spark.sparkContext.parallelize(this.individuals.takeOrdered(e))

    val offspringPopulation = offspring(selectParents(this.individuals.count-e, t), m, c)

    this.copy(individuals = offspringPopulation ++ elitePopulation).calcFitness
  }
  
  import scala.math._
  def bestFitness: Int = 
    this.individuals
      .aggregate(Int.MaxValue)(((acc, individual) => (min(acc, individual._1))),
                               ((acc1, acc2) => (min(acc1, acc2))))
}

object Population {
  def apply(populationSize: Int, 
            chromosomeSize: Long,
	    r: Random,
	    spark: SparkSession) = {
    def individuals(i: Long, list: Seq[Individual]): Seq[Individual] = 
      i match {
        case 0 => list
	case _ => individuals(i-1, Individual(chromosomeSize, r) +: list)
    }
    val individualsRDD =
      spark.sparkContext.parallelize((1 to populationSize).map(_ => (-1, Individual(chromosomeSize, r))))

    new Population(individualsRDD, spark)
  }
}
