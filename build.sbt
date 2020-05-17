import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

ThisBuild / scalaVersion := "2.11.11"
ThisBuild / version := "1.0.0"
ThisBuild / organizationName := "Spark Fireworks"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

fork in Test := true
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:+CMSClassUnloadingEnabled"
)
parallelExecution in Test := false

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

lazy val dependencies = Seq(
  // https://mvnrepository.com/artifact/org.scalatest/scalatest
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP2" % Test,
  // https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "2.4.5" % Provided,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.4.5" % Provided,
  // https://mvnrepository.com/artifact/com.google.cloud/google-cloud-storage
  "com.google.cloud" % "google-cloud-storage" % "1.108.0" % Provided,
  // https://mvnrepository.com/artifact/com.google.cloud.spark/spark-bigquery
  "com.google.cloud.spark" %% "spark-bigquery" % "0.15.1-beta" % Provided,
  // https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  // https://mvnrepository.com/artifact/com.github.scopt/scopt
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  // https://mvnrepository.com/artifact/io.spray/spray-json
  "io.spray" %% "spray-json" % "1.3.5"
)

lazy val root = (project in file("."))
  .settings(
    mainClass in(Compile, packageBin) := Some("com.ricardomiranda.magicsquare"),
    name := "Spark-on-k8s-Magic-Squares",
    libraryDependencies ++= dependencies
  )

// Simple and constant jar name
assemblyJarName in assembly := s"spark-on-k8s-magic-squares.jar"
