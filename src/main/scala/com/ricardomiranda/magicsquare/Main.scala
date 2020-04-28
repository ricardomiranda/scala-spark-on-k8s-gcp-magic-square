package com.ricardomiranda.magicsquare

import breeze.plot._

import com.ricardomiranda.magicsquare.argumentValidator.ArgumentParser
import com.typesafe.scalalogging.StrictLogging

import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.util.Random

case class Result(lineNbr: Int,
                  bestIndividualNbr: Int, 
                  newGenerationPopulationFitness: Double, 
                  newGenerationIndividualsCount: Int,
                  bestIndividualChromosome: Option[Seq[Long]])
object Main extends App with StrictLogging {

    val iter = args(0).toInt
    val sideSize = args(1).toInt
    val popSize = args(2).toInt
    val mutationRate = args(3).toDouble
    val crossoverRate = args(4).toDouble
    val elite = args(5).toInt
    val tournamentSize = args(6).toInt
    val percentile = args(7).toDouble
    val output = args(8).toInt

  /** Creates a spark session using the provided configurations.
   * Should the configurations be empty, the default spark session will be returned.
   *
   * @param configs provided configurations.
   * @return a configured spark session.
   */
  def createSparkSession(configs: Map[String, String], sparkAppName: String): SparkSession = {
    logger.info(s"Creating Spark Session with name $sparkAppName")
    val builder =
      SparkSession
        .builder()
        .appName(sparkAppName)

    Some(configs.foldLeft(builder)((accum, x) => accum.config(x._1, x._2)))
      .getOrElse(builder)
      .getOrCreate()
  }
  
  /** Method to add a file to the Spark session.
   *
   * @param filePath     The filepath to the file to add.
   * @param sparkSession The Spark session of the application.
   * @return The file path of the file in the Spark session.
   */
  def addFileToSparkContext(filePath: String, sparkSession: SparkSession): String = {
    logger.info(s"Add file ${filePath} to Spark Session")
    sparkSession.sparkContext.addFile(filePath)

    val filePathBasename: String =
      Seq(
        FilenameUtils.getBaseName(filePath),
        FilenameUtils.getExtension(filePath)).mkString(".")

    SparkFiles.get(filePathBasename)
  }

  @tailrec
  def loop(n: Int, 
           iterToGo: Int, 
           population: Population, 
           acc: Seq[Result]): Seq[Result] = iterToGo match {
  // n -> curent iteration, iterToGo -> remaining iterations,
  // population -> current population,
  // acc -> Seq[(n, bestFitness, fitnessPopulation, PopulationSize, Option[chromosome])]
  // returns -> Seq[(n, bestFitness, fitnessPopulation, PopulationSize, Option[chromosome])]
    case 0 => acc
    case _ =>
      val newGeneration = 
        population
          .newGeneration(elite, tournamentSize, mutationRate, crossoverRate)

      newGeneration.calcFitness

      val bestIndividual: (Int, Option[Individual]) = newGeneration.bestFitness match {
        case 0 => 
          val bi = newGeneration.individuals.sortByKey().first 
          (bi._1, Some(bi._2))
        case bestFitness => (bestFitness, None) 
      }

      val result: Result = 
        Result(
          lineNbr = n,
          bestIndividualNbr = bestIndividual._1, 
          newGenerationPopulationFitness = newGeneration.populationFitness(percentile), 
          newGenerationIndividualsCount = newGeneration.individuals.count.toInt,
          bestIndividualChromosome = 
            bestIndividual._2 match {
              case Some(bi) => Some(bi.chromosome)
			        case None => None
            }
          )

	    val percent = 100.0 - n.toDouble/iter.toDouble*100.0
    	if (n % output == 0) logger.info(s"Remainig: ${percent}%, best individual: ${bestIndividual._1}, population size = ${population.individuals.count}") 

      loop(n+1, 
           if (bestIndividual._1 == 0) { 0 } else { iterToGo-1 }, 
           newGeneration, 
           result+:acc)
  }

  val arguments: ArgumentParser = ArgumentParser.parser.parse(args, ArgumentParser()) match {
    case Some(config) => config
    case None => throw new IllegalArgumentException()
  }

  logger.info("All input arguments were correctly parsed")

  val sparkSession: SparkSession = createSparkSession(
    configs = arguments.configs,
    sparkAppName = arguments.sparkAppName
  )

  sparkSession.sparkContext.setLogLevel("INFO")

  val magicSquareConfigurationFile: String = addFileToSparkContext(
    filePath = arguments.magicSquareConfigurationFile,
    sparkSession = sparkSession
  )

  val iniPopulation = 
    Population(
      populationSize = popSize,
      chromosomeSize = (sideSize*sideSize).toLong,
      r = new Random(),
      spark = sparkSession
    ).calcFitness

  logger.info("Initial solution is:")
  logger.info(MagicSquare(iniPopulation.individuals.sortByKey().first._2.chromosome).m.toString)
  logger.info(s"With fitness: ${iniPopulation.individuals.sortByKey().first._1}")

  val result: Seq[Result] = loop(0, iter, iniPopulation, Seq())

  logger.info("Solution is:")

  result.head.bestIndividualChromosome match {
    case Some(chromosome) => logger.info(MagicSquare(chromosome).m.toString)
    case _ =>
  }

  logger.info(s"With fitness: ${result.head.bestIndividualNbr}")

  val fig = Figure()
  val plt = fig.subplot(0)
  plt += plot(result.map( x => x.lineNbr ), result.map( x => x.bestIndividualNbr ), name="Best individual")
  plt += plot(result.map( x => x.lineNbr ), result.map( x => x.newGenerationPopulationFitness.toInt), name="Population")
  plt.xlabel = "Iterations"
  plt.ylabel = "Fitness"
  plt.title = s"Magic square with size $sideSize fitness"
  plt.legend = true
  fig.refresh()

  logger.info("Program terminated")
  val t0 = System.nanoTime()
  val t1 = System.nanoTime()
  logger.info("Elapsed time: " + (t1 - t0) + "ns")

  sparkSession.stop()
}
