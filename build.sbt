lazy val root = (project in file(".")).
  settings(
    name := "Magic squares",
    organization := "com.marionete",
    version := "1.0",
    scalaVersion := "2.11.6"
  )

// https://mvnrepository.com/artifact/org.scalatest/scalatest_2.11
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1"

// https://mvnrepository.com/artifact/org.scalanlp/breeze_2.11
libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.13"

// https://mvnrepository.com/artifact/org.scalanlp/breeze-viz_2.11
libraryDependencies += "org.scalanlp" % "breeze-viz_2.11" % "0.13"

// https://mvnrepository.com/artifact/org.scalanlp/breeze-natives_2.11
libraryDependencies += "org.scalanlp" % "breeze-natives_2.11" % "0.13"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"



