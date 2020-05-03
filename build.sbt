ThisBuild / scalaVersion := "2.12.11"
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
  // https://mvnrepository.com/artifact/org.scalanlp/breeze
  "org.scalanlp" %% "breeze" % "1.0",
  // https://mvnrepository.com/artifact/org.scalanlp/breeze-viz
  "org.scalanlp" %% "breeze-viz" % "1.0",
  // https://mvnrepository.com/artifact/org.scalanlp/breeze-natives
  "org.scalanlp" %% "breeze-natives" % "1.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "2.4.5" % Provided,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.4.5" % Provided,
  // https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  // https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
  "ch.qos.logback" % "logback-classic" % "1.3.0-alpha5",
  // https://mvnrepository.com/artifact/com.github.scopt/scopt
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  // https://mvnrepository.com/artifact/io.spray/spray-json
  "io.spray" %% "spray-json" % "1.3.5"
)

lazy val root = (project in file("."))
  .settings(
    mainClass in (Compile, packageBin) := Some("com.ricardomiranda.sparkOnK8s"),
    name := "Spark-on-k8s-Magic-Squares",
    libraryDependencies ++= dependencies
  )

// Simple and constant jar name
assemblyJarName in assembly := s"spark-on-k8s-magic-squares.jar"
