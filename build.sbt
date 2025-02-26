ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.6.3"

resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"

lazy val circeV = "0.14.10"
lazy val jellyV = "2.6.4"
lazy val jenaV = "5.3.0"
lazy val pekkoV = "1.1.3"
lazy val pekkoHttpV = "1.1.0"
lazy val pekkoConnV = "1.1.0"
lazy val rdf4jV = "5.1.2"
lazy val icu4jV = "76.1"

lazy val root = (project in file("."))
  .settings(
    name := "dataset-ci-worker",
    idePackagePrefix := Some("io.github.riverbench.ci_worker"),

    // Scala 3 or not Scala at all
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "33.4.0-jre",
      "com.ibm.icu" % "icu4j" % icu4jV,
      "eu.ostrzyciel.jelly" %% "jelly-stream" % jellyV,
      "eu.ostrzyciel.jelly" %% "jelly-jena" % jellyV,
      "io.circe" %% "circe-core" % circeV,
      "io.circe" %% "circe-generic" % circeV,
      "io.circe" %% "circe-parser" % circeV,
      "org.apache.commons" % "commons-compress" % "1.27.1",
      "org.apache.jena" % "jena-core" % jenaV,
      "org.apache.jena" % "jena-arq" % jenaV,
      "org.apache.jena" % "jena-shacl" % jenaV,
      "org.apache.jena" % "jena-rdfconnection" % jenaV,
      "org.apache.pekko" %% "pekko-connectors-file" % pekkoConnV,
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoV,
      "org.apache.pekko" %% "pekko-stream-typed" % pekkoV,
      "org.apache.pekko" %% "pekko-http" % pekkoHttpV,
      "org.apache.pekko" %% "pekko-http-core" % pekkoHttpV,
      "org.eclipse.rdf4j" % "rdf4j-model" % rdf4jV,
      "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % rdf4jV,
      "org.eclipse.rdf4j" % "rdf4j-rio-trig" % rdf4jV,
      "org.apache.datasketches" % "datasketches-java" % "7.0.1",
    ),

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
      case PathList("redirect_template.html") => MergeStrategy.concat
      case PathList("sparql", xs @ _*) => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    assembly / assemblyOutputPath := file("target/assembly/ci-worker-assembly.jar"),

    // emit deprecated warnings
    scalacOptions ++= Seq(
      "-deprecation",
      "-Werror",
    ),
  )
