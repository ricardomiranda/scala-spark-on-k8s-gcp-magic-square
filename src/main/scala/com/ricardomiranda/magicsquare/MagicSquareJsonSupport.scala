package com.ricardomiranda.magicsquare

import com.typesafe.scalalogging.StrictLogging
import spray.json.DefaultJsonProtocol

object MagicSquareJsonSupport extends DefaultJsonProtocol with StrictLogging {

  import spray.json._

  implicit val GcpPersistenceFormat: RootJsonFormat[GcpPersistence] =
    jsonFormat2(GcpPersistence)

  implicit val MagicSquareConfigurationFormat
      : RootJsonFormat[MagicSquareConfiguration] =
    jsonFormat9(MagicSquareConfiguration)

  case class GcpPersistence(gcp_dataset: String, table: String)

  case class MagicSquareConfiguration(
      crossoverRate: Double,
      elite: Int,
      iter: Int,
      mutationRate: Double,
      percentile: Double,
      persistence: GcpPersistence,
      popSize: Int,
      sideSize: Int,
      tournamentSize: Int
  )

  /** Method to convert the contents of a configuration file into an object of
    * type MagicSquareConfiguration
    *
    * @param configurationFilePath The file path of the configuration.
    * @return Some(MagicSquareConfiguration) if the file path exists or None if not.
    */
  def magicSquareConfigFileContentsToObject(
      configurationFilePath: String
  ): MagicSquareConfiguration = {
    logger.info(s"Converting config file - $configurationFilePath - to object")
    Core
      .getFileContents(filePath = configurationFilePath)
      .get
      .parseJson
      .convertTo[MagicSquareConfiguration]
  }
}
