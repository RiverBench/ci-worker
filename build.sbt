ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.2.2"

lazy val jenaV = "4.7.0"

lazy val root = (project in file("."))
  .settings(
    name := "dataset-ci-worker",
    idePackagePrefix := Some("io.github.riverbench.ci_worker"),

      // Scala 3 or not Scala at all
    libraryDependencies ++= Seq(
      "org.apache.jena" % "jena-core" % jenaV,
      "org.apache.jena" % "jena-arq" % jenaV,
      "org.apache.jena" % "jena-shacl" % jenaV,
    ),

    // Discard module-info.class files
    // Just Java Things (tm), I guess
    assembly / assemblyMergeStrategy := {
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case PathList("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    assembly / assemblyOutputPath := file("target/assembly/ci-worker-assembly.jar"),
  )
