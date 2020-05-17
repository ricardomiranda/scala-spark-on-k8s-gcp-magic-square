package com.ricardomiranda.magicsquare.core

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

object CoreBigQuery extends StrictLogging {

  /** Method to insert data into a Big Query table.
    *
    * @param dataFrame  The DataFrame to insert.
    * @param table      The table to insert into.
    * @param writeMode  The write mode to use.
    * @param partitions The partitions of the table.
    */
  def insertIntoBigQueryTable(
      dataFrame: DataFrame,
      table: String,
      writeMode: String = "append",
      partitions: Seq[String] = Seq()
  ): Unit = {
    logger.info(s"Inserting data into Big Query table $table")
    import com.google.cloud.spark.bigquery._
    if (partitions.equals(Seq()))
      dataFrame.write.mode(writeMode).bigquery(table = table)
    else
      dataFrame.write
        .mode(writeMode)
        .partitionBy(partitions: _*)
        .bigquery(table = table)
  }
}
