package io.github.riverbench.ci_worker
package commands

object Command:
  val commands: Seq[Command] = Seq(
    HelpCommand,
    ValidateRepo,
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
  def name: String
  def description: String
  def validateArgs(args: Array[String]): Boolean
  def run(args: Array[String]): Unit
