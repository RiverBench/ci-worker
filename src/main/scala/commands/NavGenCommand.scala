package io.github.riverbench.ci_worker
package commands

import util.YamlDocBuilder
import util.YamlDocBuilder.*

import java.lang.module.ModuleDescriptor.Version
import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object NavGenCommand extends Command:
  override def name: String = "nav-gen"

  override def description: String = "Generate navigation for the documentation\n" +
    "Args: <doc repo dir> <out config file>"

  override def validateArgs(args: Array[String]) = args.length == 3

  override def run(args: Array[String]) = Future {
    val docRepoDir = FileSystems.getDefault.getPath(args(1))
    val outConfigFile = FileSystems.getDefault.getPath(args(2))
    val inConfigFile = docRepoDir.resolve("mkdocs.yml")

    val rootDir = docRepoDir.resolve("docs")

    println("Building nav structure...")
    val suiteDir = rootDir.resolve("v")
    val suites = suiteDir.toFile.listFiles()
      .filter(_.isDirectory)
      .map(sDir =>
        YamlMap(sDir.getName, YamlList(listDir(rootDir, f"v/${sDir.getName}")))
      )
      .sortBy(m => Version.parse(m.v.keys.head))
      .reverse
      .toSeq

    val profileDir = rootDir.resolve("profiles")
    val profiles = profileDir.toFile.listFiles()
      .filter(_.isDirectory)
      .map(pDir => YamlMap(
        pDir.getName,
        YamlList(
          YamlMap("Development version", f"profiles/${pDir.getName}/dev.md") +:
            listDir(rootDir, f"profiles/${pDir.getName}", false)
              .filter(v => v.isInstanceOf[YamlMap])
              .map(_.asInstanceOf[YamlMap])
              .filter(_.v.keys.head != "dev")
              .sortBy(m => Version.parse(m.v.keys.head))
              .reverse
        )
      ))
      .sortBy(_.v.keys.head)
      .toSeq

    val datasetDir = rootDir.resolve("datasets")
    val datasets = datasetDir.toFile.listFiles()
      .filter(_.isDirectory)
      .map(dDir => YamlMap(
        dDir.getName,
        YamlList(
          YamlMap(
            "Development version",
            YamlList(listDir(rootDir, f"datasets/${dDir.getName}/dev"))
          ) +: dDir.listFiles()
            .filter(_.isDirectory)
            .filter(_.getName != "dev")
            .map(dvDir => YamlMap(
              dvDir.getName,
              YamlList(listDir(rootDir, f"datasets/${dDir.getName}/${dvDir.getName}"))
            ))
            .sortBy(m => Version.parse(m.v.keys.head))
            .reverse
            .toSeq
        )
      ))
      .sortBy(_.v.keys.head)
      .toSeq

    val schemaNames = Map(
      "documentation" -> "Documentation ontology",
      "metadata" -> "Metadata ontology",
      "theme" -> "Topic scheme",
    )
    val schemaDir = rootDir.resolve("schema")
    val schemas = schemaDir.toFile.listFiles()
      .filter(_.isDirectory)
      .map(pDir => YamlMap(
        schemaNames.getOrElse(pDir.getName, pDir.getName),
        YamlList(
          YamlMap("Development version", f"schema/${pDir.getName}/dev.md") +:
            listDir(rootDir, f"schema/${pDir.getName}", false)
              .filter(v => v.isInstanceOf[YamlMap])
              .map(_.asInstanceOf[YamlMap])
              .filter(_.v.keys.head != "dev")
              .sortBy(m => Version.parse(m.v.keys.head))
              .reverse
        )
      ))
      .sortBy(_.v.keys.head)
      .toSeq

    println("Serializing...")
    val nav = YamlDocBuilder.build(YamlMap(Map(
      "nav" -> YamlList(Seq(
        YamlMap("Home", "index.md"),
        YamlMap(
          "Documentation",
          YamlList(listDir(rootDir, "documentation"))
        ),
        YamlMap("Datasets", YamlList(
          YamlString("datasets/index.md") +: datasets
        )),
        YamlMap("Profiles", YamlList(
          YamlString("profiles/index.md") +: profiles
        )),
        YamlMap("Suite releases", YamlList(suites)),
        YamlMap("Schemas & ontologies", YamlList(
          YamlString("schema/index.md") +: schemas,
        )),
      ))
    )))

    Files.writeString(
      outConfigFile,
      Files.readString(inConfigFile) + "\n\n" + nav
    )
    println("Done!")
  }

  private def listDir(rootDir: Path, dir: String, prettify: Boolean = true): Seq[YamlValue] =
    rootDir.resolve(dir).toFile.listFiles()
      .flatMap {
        case f if f.isDirectory => Some(YamlMap(
          if prettify then prettifyName(f.getName) else f.getName.stripSuffix(".md"),
          YamlList(listDir(rootDir, (dir + "/" + f.getName).stripPrefix("/")))
        ))
        case f if f.isFile && f.getName.endsWith(".md") =>
          val localPath = dir + "/" + f.getName
          if f.getName == "index.md" then Some(YamlString(dir + "/index.md"))
          else Some(YamlMap(getNameForFile(f.toPath, prettify), localPath))
        case _ => None
      }
      .toSeq

  private def getNameForFile(path: Path, prettify: Boolean): String =
    if !prettify then
      path.getFileName.toString.stripSuffix(".md")
    else
      val fallback = prettifyName(path.getFileName.toString)
      Files.lines(path).limit(2)
        .filter(_.startsWith("# "))
        .map(_.stripPrefix("# ").strip)
        .findFirst()
        .orElse(fallback)

  private def prettifyName(s: String): String =
    s.stripSuffix(".md")
      .replace("-", " ")
      .replace("_", " ")
      .capitalize
