package com.ricardomiranda.magicsquare.argumentValidator

import java.nio.file.{Files, Paths}

import com.google.cloud.storage.Storage.{BucketField, BucketGetOption}
import com.google.cloud.storage.{Storage, StorageOptions}
import scopt.OptionParser

import scala.util.Try

/** Case class for the config object.
  *
  * @param configs                      Configurations for the Spark session.
  * @param magicSquareConfigurationFile The path to the json file with the Magic
  *                                     Square  configuration.
  */
case class ArgumentParser(
                           configs: Map[String, String] = Map(),
                           magicSquareConfigurationFile: String = "",
                           sparkAppName: String = "spark_magic_quare"
                         )

/** Object that parses the arguments received. */
object ArgumentParser {

  val parser: OptionParser[ArgumentParser] =
    new scopt.OptionParser[ArgumentParser](programName = "SparkMagicSquare") {
      head(xs = "Spark Magic Square")

      opt[String]('n', name = "sparkAppName")
        .valueName("Magic Square Application Name")
        .optional()
        .action((x, c) => c.copy(sparkAppName = x))
        .text("Spark Magic Square application name.")

      opt[String]('m', name = "magicSquareConfigurationFile")
        .valueName("magicSquareConfigurationFile")
        .required()
        .action((x, c) => c.copy(magicSquareConfigurationFile = x))
        .validate(x =>
          if (validateConfigFileExistance(value = x)) {
            success
          } else {
            failure(msg = "Magic Square JSON file does not exits")
          }
        )
        .text("magicSquareConfigurationFile")

      opt[Map[String, String]]('c', name = "configs")
        .valueName("configs")
        .optional()
        .action((x, c) => c.copy(configs = x))
        .text("configs")
    }

  /** Method to check if the value passed as argument exists, either locally or
    * remotely.
    *
    * @param value The argument to check.
    * @return True if the file exists, false if not.
    */
  def validateConfigFileExistance(value: String): Boolean = {
    isLocalFileExists(filePath = value) || isRemoteAddressExists(address =
      value
    )
  }

  /** Method to check if the json file received exists.
    *
    * @param filePath the json file to check.
    * @return true if the file exists, false if not.
    */
  def isLocalFileExists(filePath: String): Boolean =
    Files.exists(Paths.get(filePath))

  /** Method that checks if a remote address exists and is reachable.
    *
    * @param address The address to check.
    * @return True if the address exists, false if not.
    */
  def isRemoteAddressExists(address: String): Boolean =
    address match {
      case x if x.startsWith("gs://") =>
        val storage: Storage = StorageOptions.getDefaultInstance().getService()
        Try {
          (storage.get(x, BucketGetOption.fields(BucketField.ID)) != null)
        }.toOption match {
          case Some(x) => true
          case None => false
        }
      case _ => false
    }
}
