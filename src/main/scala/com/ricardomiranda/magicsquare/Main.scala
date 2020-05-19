package com.ricardomiranda.magicsquare

import java.util.UUID

import com.ricardomiranda.magicsquare.argumentValidator.ArgumentParser
import com.ricardomiranda.magicsquare.core.CoreBigQuery
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

case class Result(
                   bestIndividualChromosome: Seq[Long],
                   fitness: Long,
                   lineNbr: Int,
                   populationFitness: Double,
                   runID: String
                 )

object Main extends App with StrictLogging {

  // Start timer to check running time
  val t0 = System.nanoTime()

  val arguments: ArgumentParser =
    ArgumentParser.parser.parse(args, ArgumentParser()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException()
    }

  logger.info("All input arguments were correctly parsed")

  // ID for current run
  val runID: String = UUID.randomUUID.toString

  val sparkSession: SparkSession = Computation.createSparkSession(
    configs = arguments.configs,
    sparkAppName = arguments.sparkAppName
  )

  sparkSession.sparkContext.setLogLevel("INFO")

  val magicSquareConfigurationFilePath: String =
    Computation.addFileToSparkContext(
      filePath = arguments.magicSquareConfigurationFile,
      sparkSession = sparkSession
    )

  val magicSquareConfigs: MagicSquareJsonSupport.MagicSquareConfiguration =
    MagicSquareJsonSupport
      .magicSquareConfigFileContentsToObject(configurationFilePath =
        magicSquareConfigurationFilePath
      )

  val iniPopulation: Population =
    Population(
      chromosomeSize =
        (magicSquareConfigs.sideSize * magicSquareConfigs.sideSize).toLong,
      populationSize = magicSquareConfigs.popSize,
      sparkSession = sparkSession
    )

  val b: Option[(Seq[Long], Long)] = iniPopulation.fitestIndividual
  val firstResult: Result = Result(
    bestIndividualChromosome = b.get._1,
    fitness = b.get._2,
    lineNbr = 0,
    populationFitness = iniPopulation
      .populationFitness(percentile = magicSquareConfigs.percentile, popSize = magicSquareConfigs.popSize)
      .get,
    runID = runID
  )

  // Solving the problem
  val results: Seq[Result] = Computation.loop(
    acc = Seq(firstResult),
    iterToGo = magicSquareConfigs.iter,
    n = 1,
    magicSquareConfigs = magicSquareConfigs,
    population = iniPopulation,
    runID = runID
  )

  Computation.finalOutput(
    magicSquareConfigs = magicSquareConfigs,
    results = results,
    runID = runID,
    sparkSession = sparkSession
  )

  logger.info("Program terminated")
  val t1 = System.nanoTime()
  logger.info("Elapsed time: " + (t1 - t0) + "ns")

  sparkSession.stop()
}

case object Computation extends StrictLogging {

  /**
    * Method to add a file to the Spark session.
    *
    * @param filePath     The filepath to the file to add.
    * @param sparkSession The Spark session of the application.
    * @return The file path of the file in the Spark session.
    */
  def addFileToSparkContext(
                             filePath: String,
                             sparkSession: SparkSession
                           ): String = {
    logger.info(s"Add file ${filePath} to Spark Session")

    sparkSession.sparkContext.addFile(filePath)
    val filePathBasename: String =
      Seq(
        FilenameUtils.getBaseName(filePath),
        FilenameUtils.getExtension(filePath)
      ).mkString(".")

    SparkFiles.get(filePathBasename)
  }

  /**
    * Creates a spark session using the provided configurations.
    * Should the configurations be empty, the default spark session will be 
    * returned.
    *
    * @param configs provided configurations.
    * @return a configured spark session.
    */
  def createSparkSession(
                          configs: Map[String, String],
                          sparkAppName: String
                        ): SparkSession = {
    logger.info(s"Creating Spark Session with name $sparkAppName")

    val builder =
      SparkSession
        .builder()
        .appName(sparkAppName)

    Some(configs.foldLeft(builder)((accum, x) => accum.config(x._1, x._2)))
      .getOrElse(builder)
      .getOrCreate()
  }

  /**
    * Print final program results
    *
    * @param results Sequence o Results stroed during the computation
    */
  def finalOutput(
                   magicSquareConfigs: MagicSquareJsonSupport.MagicSquareConfiguration,
                   results: Seq[Result],
                   runID: String,
                   sparkSession: SparkSession
                 ): Unit = {
    logger.info(s"Writing results for run: ${runID}")
    import sparkSession.implicits._
    val df: DataFrame = results.toDF
    val table: String =
      s"${magicSquareConfigs.persistence.gcp_dataset}.${magicSquareConfigs.persistence.table}"
    CoreBigQuery.insertIntoBigQueryTable(dataFrame = df, table = table)
  }

  /**
    * This the main program loop
    *
    * @param n          curent iteration
    * @param iterToGo   remaining iterations
    * @param population current population
    * @param acc        Seq[Result]
    * @return Seq[Result]
    */
  @tailrec
  def loop(
            acc: Seq[Result],
            iterToGo: Int,
            n: Int,
            magicSquareConfigs: MagicSquareJsonSupport.MagicSquareConfiguration,
            population: Population,
            runID: String
          ): Seq[Result] = iterToGo match {

    case 0 => acc
    case _ =>
      val newGeneration: Population =
        population
          .newGeneration(
            crossoverRate = magicSquareConfigs.crossoverRate,
            elite = magicSquareConfigs.elite,
            mutationRate = magicSquareConfigs.mutationRate,
            popSize = magicSquareConfigs.popSize,
            tournamentSize = magicSquareConfigs.tournamentSize
          )

      val b: Option[(Seq[Long], Long)] = newGeneration.fitestIndividual
      val result: Result =
        Result(
          bestIndividualChromosome = b.get._1,
          fitness = b.get._2,
          lineNbr = n,
          populationFitness = newGeneration
            .populationFitness(percentile = magicSquareConfigs.percentile, popSize = magicSquareConfigs.popSize)
            .get,
          runID = runID
        )

      // Continues if solution was not found and did not reach the max number 
      // of iterations
      loop(
        acc = result +: acc,
        iterToGo = if (b.get._2 <= 0) {
          0
        }
        else {
          iterToGo - 1
        },
        n = n + 1,
        magicSquareConfigs = magicSquareConfigs,
        population = newGeneration,
        runID = runID
      )
  }
}
