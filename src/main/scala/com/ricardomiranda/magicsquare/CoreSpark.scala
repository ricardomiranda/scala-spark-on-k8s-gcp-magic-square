package com.ricardomiranda.magicsquare

import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{Column, DataFrame}

object CoreSpark {
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
