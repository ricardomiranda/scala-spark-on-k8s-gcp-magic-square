package com.ricardomiranda.magicsquare

import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{Column, DataFrame}

object CoreSpark {

  /** Method to create a DataFrame hash 
   * 
   * @param dataFrame  The DataFrame to hash
   * @return A String with a hash for the DataFrame
   */
  def hashed_dataframe(df: DataFrame): Int = {
    val selection: Array[Column] = df.columns.map(col)
    val joined_values: String = "joined_values"
    (df.columns.mkString("_") +
      df.withColumn(joined_values, concat_ws(sep = "_", selection: _*))
        .select(joined_values)
        .collect
        .foldLeft("") { (acc, x) => acc + x(0) }).hashCode
  }
}
