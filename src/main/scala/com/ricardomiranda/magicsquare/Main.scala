package com.ricardomiranda.magicsquare

import breeze.plot._

import com.ricardomiranda.magicsquare.argumentValidator.ArgumentParser
import com.typesafe.scalalogging.StrictLogging

import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.util.Random

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
  def loop(n: Int, iterToGo: Int, population: Population, acc: Seq[(Int, Int, Double, Int, Option[Seq[Long]])]): 
  Seq[(Int, Int, Double, Int, Option[Seq[Long]])] = iterToGo match {
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

      val result = (n,
                    bestIndividual._1, 
                    newGeneration.populationFitness(percentile), 
                    newGeneration.individuals.count.toInt,
                    bestIndividual._2 match {
                        case Some(bi) => Some(bi.chromosome)
			                  case None => None
                      }
                    )

	    val percent = 100.0 - n.toDouble/iter.toDouble*100.0
    	if (n % output == 0) println(s"Remainig: ${percent}%, best individual: ${bestIndividual._1}, population size = ${population.individuals.count}") 

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

  val result = loop(0, iter, iniPopulation, Seq())

  logger.info("Solution is:")

  result.head._5 match {
    case Some(chromosome) => logger.info(MagicSquare(chromosome).m.toString)
    case _ =>
  }

  logger.info(s"With fitness: ${result.head._2}")

  val fig = Figure()
  val plt = fig.subplot(0)
  plt += plot(result.map(_._1), result.map(_._2), name="Best individual")
  plt += plot(result.map(_._1), result.map(_._3.toInt), name="Population")
  plt.xlabel = "Iterations"
  plt.ylabel = "Fitness"
  plt.title = s"Magic square with size $sideSize fitness"
  plt.legend = true
  fig.refresh()

  println("Program terminated")
  val t0 = System.nanoTime()
  val t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0) + "ns")

  sparkSession.stop()
}
