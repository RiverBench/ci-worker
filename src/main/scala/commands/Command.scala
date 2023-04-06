package io.github.riverbench.ci_worker
package commands

import akka.actor.typed.ActorSystem
import io.github.riverbench.ci_worker.Global

import scala.concurrent.ExecutionContext

object Command:
  val commands: Seq[Command] = Seq(
    HelpCommand,
    ValidateRepoCommand,
    PackageCommand,
  )

  def getCommand(name: Option[String]): Command =
    name match
      case Some(name) => commands.find(_.name == name).getOrElse(ErrorCommand)
      case None => ErrorCommand

  def runCommand(args: Array[String]): Unit =
    val command = getCommand(args.headOption)
    command.validateArgs(args) match
      case true => command.run(args)
      case false => println(command.description)

trait Command:
  implicit val actorSystem: ActorSystem[_] = Global.actorSystem
  implicit val executionContext: ExecutionContext = actorSystem.executionContext

  def name: String
  def description: String
  def validateArgs(args: Array[String]): Boolean
  def run(args: Array[String]): Unit
