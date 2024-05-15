package io.github.riverbench.ci_worker
package commands

import util.YamlDocBuilder
import util.YamlDocBuilder.*

import java.lang.module.ModuleDescriptor.Version
import java.nio.file.{FileSystems, Files, Path}
import scala.concurrent.Future

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

    val taskDir = rootDir.resolve("tasks")
    val tasks = listDir(rootDir, f"tasks", false)
      .collect { case m: YamlMap => (
        m.v.keys.head,
        YamlMap(
          getNameForFile(taskDir.resolve(m.v.keys.head + "/index.md"), true),
          m.v.values.head
        )
      ) }
      .sortBy(_._1)

    val profileDir = rootDir.resolve("profiles")
    val profiles = listDir(rootDir, f"profiles", false)
      .collect { case m: YamlMap => m }
      .sortBy(_.v.keys.head)

    val categoryDir = rootDir.resolve("categories")
    val categories = categoryDir.toFile.listFiles()
      .filter(_.isDirectory)
      .sortBy(_.getName)
      .map(cDir => YamlMap(
        getNameForFile(cDir.toPath.resolve("index.md"), true),
        YamlList(
          YamlString(f"categories/${cDir.getName}/index.md") +:
            listDir(rootDir, f"categories/${cDir.getName}")
              .collect { case m: YamlMap => m }
              .filter(m => {
                val value = m.v.values.head.asInstanceOf[YamlString].v
                !value.contains("profile_table.md") && !value.contains("task_table.md")
              })
              .sortBy(m => m.v.keys.head) :+
            YamlMap("Benchmark tasks", YamlList(
              tasks.filter(_._1.startsWith(cDir.getName)).map(_._2)
            )) :+
            YamlMap("Benchmark profiles", YamlList(
              profiles.filter(_.v.keys.head.startsWith(cDir.getName))
            ))
        )
      ))
      .toSeq

    val datasetDir = rootDir.resolve("datasets")
    val datasets = datasetDir.toFile.listFiles()
      .filter(_.isDirectory)
      .map(dDir => YamlMap(dDir.getName, f"datasets/${dDir.getName}/index.md"))
      .sortBy(_.v.keys.head)
      .toSeq

    val schemaNames = Map(
      "documentation.md" -> "Documentation ontology",
      "metadata.md" -> "Metadata ontology",
    )
    val schemaDir = rootDir.resolve("schema")
    val schemas = schemaDir.toFile.listFiles()
      .filter(f => f.isFile && f.getName != "index.md" && f.getName.endsWith(".md"))
      .map(pFile => YamlMap(
        schemaNames.getOrElse(pFile.getName, pFile.getName),
        f"schema/${pFile.getName}"
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
        YamlMap("Benchmarks", YamlList(
          categories
        )),
        YamlMap("Datasets", YamlList(
          YamlString("datasets/index.md") +: datasets
        )),
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
        case f if f.isDirectory =>
          val list = YamlList(listDir(rootDir, (dir + "/" + f.getName).stripPrefix("/")))
          if list.v.isEmpty then None else Some(YamlMap(
            if prettify then prettifyName(f.getName) else f.getName.stripSuffix(".md"),
            list
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
