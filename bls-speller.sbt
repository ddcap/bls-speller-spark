name := "bls-speller"

version := "0.1"

scalaVersion := "2.11.12"

// Apache Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"
// CLI parsing
libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0"
libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.15"

// Test framework
// From https://www.scalatest.org/user_guide/using_scalatest_with_sbt
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
logBuffered in Test := false
// From http://www.scalacheck.org/
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.1" % "test"
// use spark-testing-base https://github.com/holdenk/spark-testing-base
// TODO make dependent on Spark version
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test"
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

test in assembly := {}
excludeFilter in unmanagedSources := "test-code.scala"
