package io.github.riverbench.ci_worker
package commands

import scala.concurrent.Future

object HelpCommand extends Command:
  override def name: String = "help"

  override def description: String = "Prints this help message"

  override def validateArgs(args: Array[String]) = true

  override def run(args: Array[String]): Future[Unit] =
    Future {
      println("Available commands:")
      Command.commands.foreach { command =>
        println(s"* ${command.name} - ${command.description}\n")
      }
    }
