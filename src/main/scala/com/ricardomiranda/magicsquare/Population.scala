package com.ricardomiranda.magicsquare

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{sum, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.WrappedArray
import scala.util.Random

case class Population(individuals: DataFrame, sparkSession: SparkSession)
  extends StrictLogging {

  /**
    * Find the fitest individual and returns its fitness
    *
    * @return Option Fitest individual chromosome and fitness
    */
  def fitestIndividual: Option[(Seq[Long], Long)] =
    this.individuals match {
      case df if df.isEmpty => None
      case df =>
        logger.debug(s"Computing top fitness")
        val r: Row =
          df
            .orderBy("fitness")
            .limit(1)
            .head

        Some((r.getAs[WrappedArray[Long]](0), r.getAs[Long](1)))
    }

  /**
    * Compute a new generation
    *
    * @param crossoverRate   crossover rate
    * @param elite           elite
    * @param mutationRate    mutation rate
    * @param randomGenerator Random generator
    * @param tournamentSize  tournament size
    * @return
    */
  def newGeneration(
                     crossoverRate: Double,
                     elite: Int,
                     mutationRate: Double,
                     randomGenerator: Random = new Random,
                     popSize: Long,
                     tournamentSize: Int
                   ): Population = {

    // Checkpointing Population to reduce DAG
    this.individuals.cache()
    //    val checkpointDir: String = "/tmp/checkpoints"
    //    sparkSession.sparkContext.setCheckpointDir(checkpointDir)
    //    val checkPointedPopulation: Dataset[Row] =  this.individuals.checkpoint(eager = true)

    val elitePopulation: DataFrame = this.individuals.orderBy("fitness").limit(elite)

    val offspringPopulation: DataFrame = (popSize - elite) match {
      case n if n == 0 =>
        sparkSession.createDataFrame(
          sparkSession.sparkContext.emptyRDD[Row],
          elitePopulation.schema
        )
      case n =>
        this.offspring(
          crossoverRate = crossoverRate,
          mutationRate = mutationRate,
          parents =
            this.selectParents(
              nbrOfOffspring = n,
              popSize = popSize,
              randomGenerator = randomGenerator,
              tournamentSize = tournamentSize
            )
              .get,
          randomGenerator = randomGenerator
        )
    }

    this.individuals.unpersist()
    this.copy(individuals = offspringPopulation.union(elitePopulation))
  }

  /**
    * Offspring part of the population
    *
    * @param crossoverRate   crossover rate
    * @param mutationRate    mutation rate
    * @param parents         DataFrame with 2 parents in each line
    * @param randomGenerator Random generator
    * @return DataFrame with offspring part of the population
    */
  def offspring(
                 crossoverRate: Double,
                 mutationRate: Double,
                 parents: DataFrame,
                 randomGenerator: Random = new Random
               ): DataFrame = {
    logger.debug(
      s"Generating offspring for the new generation."
    )

    val crossoverUDF: UserDefinedFunction = udf(Individual.crossover(crossoverRate)(randomGenerator))

    val fUDF: UserDefinedFunction = udf(Individual.calcFitness compose Individual.mutation(mutationRate)(randomGenerator))

    val df: DataFrame =
      parents
        .withColumn(
          "chromosome",
          crossoverUDF(parents.col("p1"), parents.col("p2"))
        )

    df.withColumn("fitness", fUDF(df.col("chromosome")))
      .drop("p1")
      .drop("p2")
  }

  /**
    * Selects parents
    *
    * @param nbrOfOffspring  number of children
    * @param popSize         Population size
    * @param randomGenerator Random generator
    * @param tournamentSize  tournament size
    * @return Option[DataFrame] with columns "p1" and "p2"
    */
  def selectParents(
                     nbrOfOffspring: Long,
                     popSize: Long,
                     randomGenerator: Random = new Random,
                     tournamentSize: Int
                   ): Option[DataFrame] = {

    def eachParent(colName: String): DataFrame = {

      val struct: StructType = StructType(
        Seq(
          StructField(colName, ArrayType(LongType, false), true),
          StructField("id", LongType, false)
        )
      )

      val pRDD: RDD[(Row, Long)] =
        this.tournamentSelection(
          nbrOfParents = nbrOfOffspring,
          popSize = popSize,
          randomGenerator = randomGenerator,
          tournamentSize = tournamentSize
        ).get
          .zipWithIndex

      sparkSession.createDataFrame(pRDD.map { case (r, i) => Row.fromSeq(r.toSeq ++ Seq(i)) }, struct)
    }

    popSize match {
      case x if x < 2 => None
      case _ =>
        logger.debug(
          s"""
          Selecting parents for ${nbrOfOffspring} offspring with
          tournament size: ${tournamentSize}"""
        )

        Some(
          eachParent(colName = "p1")
            .join(eachParent(colName = "p2"), Seq("id"))
            .drop(colName = "id")
        )
    }
  }

  /** Tournament selection selects its parents by running a series of "tournaments".
    * First, individuals are randomly selected from the population and entered into a
    * tournament. Next, these individuals can be thought to compete with each other
    * by comparing their fitness values, then choosing the individual with the highest
    * fitness for the parent.
    *
    * @param nbrOfParents    Number of parents to select
    * @param popSize         Population size
    * @param randomGenerator Random generator
    * @param tournamentSize  tournament size
    * @return Option Dataframe with selected Individuals
    */
  def tournamentSelection(
                           nbrOfParents: Long,
                           popSize: Long,
                           randomGenerator: Random = new Random,
                           tournamentSize: Int
                         ): Option[RDD[Row]] =

    popSize match {
      case x if x < 2 => None
      case x =>
        logger.debug(
          s"Tournament selection with tournament size: ${tournamentSize}"
        )
        val seed: Long = randomGenerator.nextLong()

        Some((1L to nbrOfParents)
          .map(_ =>
            this.individuals
              .sample(false, tournamentSize.toDouble / x.toDouble, seed)
              .limit(tournamentSize)
              .orderBy("fitness")
              .limit(1)
              .drop("fitness")
              .rdd
          ).reduce((x, y) => x ++ y))
    }

  /**
    * Computes Population fitness based on a percentile
    *
    * @param percentile Percentile of the Population to use to compute fitness
    * @return Option of Population fitness
    */
  def populationFitness(percentile: Double, popSize: Long): Option[Double] =
    this.individuals match {
      case df if df.isEmpty => None
      case df =>
        val percentilePop: Int = (percentile * popSize.toDouble).toInt + 1
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
}

object Population {

  /**
    * Population contructor
    *
    * @param chromosomeSize  Chromosome size (side * side)
    * @param populationSize  Population size
    * @param randomGenerator Random generator
    * @param sparkSession    SparkSession
    * @return Population
    */
  def apply(
             chromosomeSize: Long,
             populationSize: Int,
             randomGenerator: Random = new Random,
             sparkSession: SparkSession
           ): Population = {

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
