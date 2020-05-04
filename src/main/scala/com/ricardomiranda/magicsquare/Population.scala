package com.ricardomiranda.magicsquare

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{min, max, rand, sum}
import org.apache.spark.sql.types._

import scala.util.Random
import com.typesafe.scalalogging.StrictLogging
import breeze.util.Opt

case class Population(individuals: DataFrame, sparkSession: SparkSession)
    extends StrictLogging {

  /**
    * Find the fitest individual and returns its fitness
    *
    * @return Option Fitest individual fitness
    */
  def fitestIndividual: Option[Long] =
    this.individuals match {
      case df if df.isEmpty => None
      case df =>
        logger.debug(s"Computing top fitness")
        Some(this.individuals.agg(min("fitness")).head.getLong(0))
    }

  // /**
  //   * Compute a new generation
  //   *
  //   * @param crossoverRate  crossover rate
  //   * @param elite          elite
  //   * @param mutationRate   mutation rate
  //   * @param tournamentSize tournament size
  //   * @return
  //   */
  // def newGeneration(
  //     crossoverRate: Double,
  //     elite: Int,
  //     mutationRate: Double,
  //     tournamentSize: Int
  // ): Population = {
  //   val elitePopulation: DataFrame = this.individuals.orderBy(rand()).limit(e)

  //   val offspringPopulation: DataFrame =
  //     offspring(
  //       selectParents(this.individuals.count - elite, tournamentSize),
  //       mutationRate,
  //       crossoverRate
  //     )

  //   this.copy(individuals = offspringPopulation.join(elitePopulation))
  // }

  /**
    * Computes Population fitness based on a percentile
    *
    * @param percentile Percentile of the Population to use to compute fitness
    * @return Option of Population fitness
    */
  def populationFitness(percentile: Double): Option[Double] =
    this.individuals match {
      case df if df.isEmpty => None
      case df =>
        val percentilePop: Int = (percentile * df.count).toInt + 1
        logger.debug(
          s"Computing Population fitness for percentile: ${percentile}"
        )

        Some(
          df.orderBy("fitness")
            .limit(percentilePop)
            .agg(sum("fitness"))
            .head
            .getLong(0) / percentilePop.toDouble
        )
    }

  // /**
  //   * Selects the parents
  //   *
  //   * @param nbrOfChildren  number of children
  //   * @param seed           seed for tournament selection
  //   * @param tournamentSize tournament size
  //   * @return DataFrame with 2 parents in each line
  //   */
  // def selectParents(
  //     nbrOfChildren: Long,
  //     seed: Int = new Random().nextInt,
  //     tournamentSize: Int
  // ): Option[DataFrame] =
  //   this.individuals match {
  //     case df if df.count < 2 => None
  //     case df =>
  //       logger.debug(
  //         s"Selecting parents for ${nbrOfChildren} children with tournament size: ${tournamentSize}"
  //       )
  //       import sparkSession.implicits._
  //       Some(
  //         sparkSession.sparkContext
  //           .parallelize(1 to nbrOfChildren.toInt)
  //           .map(_ =>
  //             (
  //               tournamentSelection(
  //                 seed = seed,
  //                 tournamentSize = tournamentSize
  //               ).get,
  //               tournamentSelection(
  //                 seed = seed,
  //                 tournamentSize = tournamentSize
  //               ).get
  //             )
  //           )
  //           .toDF("p1", "p2")
  //       )
  //   }

  /** Tournament selection selects its parents by running a series of "tournaments".
    * First, individuals are randomly selected from the population and entered into a
    * tournament. Next, these individuals can be thought to compete with each other
    * by comparing their fitness values, then choosing the individual with the highest
    * fitness for the parent.
    *
    * @param nbrOfParents   Number of parents to select (2 times the number of
    *                       offspring)
    * @param seed           seed for tournament selection
    * @param tournamentSize tournament size
    * @return Option Dataframe with selected Individuals
    */
  def tournamentSelection(
      nbrOfParents: Int,
      seed: Int = new Random().nextInt,
      tournamentSize: Int
  ): Option[DataFrame] =
    this.individuals match {
      case df if df.count < 2 => None
      case df =>
        logger.debug(
          s"Tournament selection with tournament size: ${tournamentSize}"
        )
        val rs: Seq[(Seq[Long], Long)] = (1 to nbrOfParents)
          .map(_ =>
            this.individuals
              .orderBy(rand(seed))
              .limit(tournamentSize)
              .orderBy("fitness")
              .head()
          )
          .map(r =>
            (
              Chromosome(r.getAs[Seq[Long]]("chromosome")).get.value,
              r.getAs[Long]("fitness")
            )
          )
        Some(sparkSession.createDataFrame(rs).toDF("chromosome", "fitness"))
    }
//   /**
//     * Offspring part of the population
//     *
//     * @param crossoverRate crossover rate
//     * @param mutationRate  mutation rate
//     * @param parents       DataFrame with 2 parents in each line
//     * @return DataFrame with offspring part of the population
//     *         (chromosome, fitnes, individual)
//     */
//   def offspring(
//       parents: DataFrame,
//       mutationRate: Double,
//       c: Double
//   ): DataFrame =
//     parents.withColumn(
//       "offsppring",
//       parents
//         .col("p1")
//         .crossover(
//           parents.col("p2"),
//           crossoverRate = crossoverRate,
//           new Random()
//         )
//         .mutation(mutationRate, new Random())
  // )
}

object Population {

  /**
    * Population contructor
    *
    * @param chromosomeSize  Chromosome size (side * side)
    * @param populationSize  Population size
    * @param randomGenerator Random generator
    * @param spark           SparkSession
    * @return Population
    */
  def apply(
      chromosomeSize: Long,
      populationSize: Int,
      randomGenerator: Random,
      sparkSession: SparkSession
  ) = {

    val is: scala.collection.immutable.IndexedSeq[(Seq[Long], Long)] =
      (1 to populationSize)
        .map(_ =>
          Individual(
            chromosomeSize = chromosomeSize,
            randomGenerator = randomGenerator
          ).get
        )
        .map(i => (i.chromosome.value, i.fitness))

    new Population(
      individuals = sparkSession
        .createDataFrame(is)
        .toDF("chromosome", "fitness"),
      sparkSession = sparkSession
    )
  }
}
