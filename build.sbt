ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.2.2"

lazy val akkaV = "2.6.19"
lazy val akkaHttpV = "10.2.10"
lazy val alpakkaV = "3.0.4"
lazy val circeV = "0.14.5"
lazy val jenaV = "4.7.0"
lazy val rdf4jV = "4.2.3"

lazy val root = (project in file("."))
  .settings(
    name := "dataset-ci-worker",
    idePackagePrefix := Some("io.github.riverbench.ci_worker"),

    // Scala 3 or not Scala at all
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "31.1-jre",
      "io.circe" %% "circe-core" % circeV,
      "io.circe" %% "circe-generic" % circeV,
      "io.circe" %% "circe-parser" % circeV,
      "org.apache.commons" % "commons-compress" % "1.23.0",
      "org.apache.jena" % "jena-core" % jenaV,
      "org.apache.jena" % "jena-arq" % jenaV,
      "org.apache.jena" % "jena-shacl" % jenaV,
      "org.eclipse.rdf4j" % "rdf4j-model" % rdf4jV,
      "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % rdf4jV,
      "org.eclipse.rdf4j" % "rdf4j-rio-trig" % rdf4jV,
    ),

    // Packages available only with Scala 2.13
    libraryDependencies ++= Seq(
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaV,
      "com.typesafe.akka" %% "akka-actor-typed" % akkaV,
      "com.typesafe.akka" %% "akka-stream-typed" % akkaV,
      "com.typesafe.akka" %% "akka-http" % akkaHttpV,
      "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
    ).map(_.cross(CrossVersion.for3Use2_13)),

    // Discard module-info.class files
    // Just Java Things (tm), I guess
    assembly / assemblyMergeStrategy := {
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => (xs map {_.toLowerCase}) match {
        // Merge services – otherwise RDF4J's parsers won't get registered
        case "services" :: xs => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
      case PathList("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    assembly / assemblyOutputPath := file("target/assembly/ci-worker-assembly.jar"),

    // emit deprecated warnings
    scalacOptions ++= Seq(
      "-deprecation",
    ),
  )
