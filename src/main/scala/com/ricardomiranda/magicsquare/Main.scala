package com.ricardomiranda.magicsquare

import breeze.plot._
import com.ricardomiranda.magicsquare.argumentValidator.ArgumentParser
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import scala.annotation.tailrec

case class Result(
    lineNbr: Int,
    fitness: Long,
    newGenerationPopulationFitness: Double,
    bestIndividualChromosome: Seq[Long]
)

object Main extends App with StrictLogging {

  // Start timer to check running time
  val t0 = System.nanoTime()

  val arguments: ArgumentParser =
    ArgumentParser.parser.parse(args, ArgumentParser()) match {
      case Some(config) => config
      case None         => throw new IllegalArgumentException()
    }

  logger.info("All input arguments were correctly parsed")

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

  logger.info("Initial solution is:")
  val b: Option[(Seq[Long], Long)] = iniPopulation.fitestIndividual
  logger.info(s"""
    fitness: ${b.get._2}
    ${MagicSquare.matrix(Chromosome(b.get._1).get).get.toString}
    """)

  val results: Seq[Result] = Computation.loop(
    0,
    magicSquareConfigs.iter,
    iniPopulation,
    Seq(),
    magicSquareConfigs
  )

  logger.info("Final solution is:")
  logger.info(s"""
    fitness: ${results.head.fitness}
    ${MagicSquare
    .matrix(Chromosome(results.head.bestIndividualChromosome).get)
    .get
    .toString}
      """)

  logger.info("Program terminated")
  val t1 = System.nanoTime()
  logger.info("Elapsed time: " + (t1 - t0) + "ns")

  Computation.finalOutput(
    results = results,
    sideSize = magicSquareConfigs.sideSize
  )

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
    * Should the configurations be empty, the default spark session will be returned.
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
    * @param results  Sequence o Results stroed during the computation
    * @param sideSize Magic square side size
    */
  def finalOutput(results: Seq[Result], sideSize: Int): Unit = {
    logger.info(s"With fitness: ${results.head.fitness}")

    val fig = Figure()
    val plt = fig.subplot(0)
    plt += plot(
      results.map(x => x.lineNbr),
      results.map(x => x.fitness.toInt),
      name = "Best individual"
    )
    plt += plot(
      results.map(x => x.lineNbr),
      results.map(x => x.newGenerationPopulationFitness.toInt),
      name = "Population"
    )
    plt.xlabel = "Iterations"
    plt.ylabel = "Fitness"
    plt.title = s"Magic square with size ${sideSize} fitness"
    plt.legend = true
    fig.refresh()
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
      n: Int,
      iterToGo: Int,
      population: Population,
      acc: Seq[Result],
      magicSquareConfigs: MagicSquareJsonSupport.MagicSquareConfiguration
  ): Seq[Result] = iterToGo match {

    case 0 => acc
    case _ =>
      val newGeneration: Population =
        population
          .newGeneration(
            crossoverRate = magicSquareConfigs.crossoverRate,
            elite = magicSquareConfigs.elite,
            mutationRate = magicSquareConfigs.mutationRate,
            tournamentSize = magicSquareConfigs.tournamentSize
          )

      val b: Option[(Seq[Long], Long)] = newGeneration.fitestIndividual

      val result: Result =
        Result(
          lineNbr = n,
          fitness = b.get._2,
          newGenerationPopulationFitness = newGeneration
            .populationFitness(percentile = magicSquareConfigs.percentile)
            .get,
          bestIndividualChromosome = b.get._1
        )

      val percent: Double =
        100.0 - n.toDouble / magicSquareConfigs.iter.toDouble * 100.0
      if (n % magicSquareConfigs.output == 0)
        logger.info(
          s"""
          Remainig: ${percent}%, best individual: ${b.get._2}, 
          population size = ${magicSquareConfigs.popSize}"""
        )

      loop(n + 1, if (b.get._2 == 0) { 0 }
      else { iterToGo - 1 }, newGeneration, result +: acc, magicSquareConfigs)
  }
}
