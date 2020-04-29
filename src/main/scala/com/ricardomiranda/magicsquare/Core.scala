package com.ricardomiranda.magicsquare

import com.typesafe.scalalogging.StrictLogging

import scala.io.{BufferedSource, Source}
import scala.util.Try

case object Core extends StrictLogging {

	/** Method to get the file contents of a text file.
	 *
	 * @param filePath The path of the text file.
	 * @return An Option[String] with the contents of the file or None if it
	 *         fails to read the file.
	 */
	def getFileContents(filePath: String): Option[String] = {
		logger.info(s"Getting file contents from file $filePath")
		Try {
			val bufferedSource: BufferedSource = Source.fromFile(filePath)
			val fileContents: String = bufferedSource.getLines.mkString
			bufferedSource.close()
			fileContents
		}.toOption
	}
}