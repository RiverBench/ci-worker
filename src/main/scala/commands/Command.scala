package io.github.riverbench.ci_worker
package commands

import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

object Command:
  implicit private val ec: ExecutionContext = Global.actorSystem.executionContext

  val commands: Seq[Command] = Seq(
    DatasetDocGenCommand,
    HelpCommand,
    MainDocGenCommand,
    MergeMetadataCommand,
    NavGenCommand,
    PackageCommand,
    PackageMainCommand,
    PackageSchemaCommand,
    ParseTestCommand,
    SchemaDocGenCommand,
    SelfTestCommand,
    TagDocumentationCommand,
    ValidateRepoCommand,
  )

  def getCommand(name: Option[String]): Command =
    name match
      case Some(name) => commands.find(_.name == name).getOrElse(ErrorCommand)
      case None => ErrorCommand

  def runCommand(args: Array[String]): Future[Unit] =
    val command = getCommand(args.headOption)
    command.validateArgs(args) match
      case true => command.run(args)
      case false => Future { println(command.description) }

trait Command:
  implicit val actorSystem: ActorSystem[_] = Global.actorSystem
  implicit val executionContext: ExecutionContext = actorSystem.executionContext

  def name: String
  def description: String
  def validateArgs(args: Array[String]): Boolean
  def run(args: Array[String]): Future[Unit]
