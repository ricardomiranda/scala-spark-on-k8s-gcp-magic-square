package com.marionete.magicsquare

object Main extends App {
  def helpMessage: Unit = {
    println("To run this program type:")
    println("geneticMagicSquares, max number of iterations, magic square size, size of the population, mutation rate, crossover, elite, tournament size, percentile for population fitness computation, Interval between sreen outputs")
    println("geneticMagicSquares Int Int Int Double Double Int Int Double Int")
  }

  def wrongNbrArgs: Unit = {
    println("Invalid input, invalid number of arguments")
    println("Type geneticMagicSquares --help")
  }

  def main: Unit = {
    val iter = args(0).toInt
    val sideSize = args(1).toInt
    val popSize = args(2).toInt
    val mutationRate = args(3).toDouble
    val crossoverRate = args(4).toDouble
    val elite = args(5).toInt
    val tournamentSize = args(6).toInt
    val percentile = args(7).toDouble
    val output = args(8).toInt

    import scala.annotation.tailrec
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
          population.newGeneration(elite, tournamentSize, mutationRate, crossoverRate)

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
		      })

	val percent = 100.0 - n.toDouble/iter.toDouble*100.0
	if (n % output == 0) println(s"Remainig: ${percent}%, best individual: ${bestIndividual._1}, population size = ${population.individuals.count}") 

        loop(n+1, 
             if (bestIndividual._1 == 0) { 0 } else { iterToGo-1 }, 
             newGeneration, 
             result+:acc)
    }

    import scala.util.Random
    import org.apache.spark.sql.SparkSession

    val spark: SparkSession = SparkSession.builder()
      .appName("Magic Squares wiht Spark")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val iniPopulation = Population(populationSize = popSize,
                                   chromosomeSize = (sideSize*sideSize).toLong,
                                   r = new Random(),
                                   spark = spark).calcFitness

    println("Initial solution is:")
    println(MagicSquare(iniPopulation.individuals.sortByKey().first._2.chromosome).m)
    println(s"With fitness: ${iniPopulation.individuals.sortByKey().first._1}")

    val result = loop(0, iter, iniPopulation, Seq())

    println("Final solution is:")

    result.head._5 match {
      case Some(chromosome) => println(MagicSquare(chromosome).m)
      case _ =>
    }

    println(s"With fitness: ${result.head._2}")
    import breeze.plot._
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

    spark.stop()
  }

  args match {
    case Array() => helpMessage
    case Array("--help",_*) => helpMessage
    case xs if xs.size == 9 => main
    case _ => wrongNbrArgs
  }
}
