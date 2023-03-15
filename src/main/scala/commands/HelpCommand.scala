package io.github.riverbench.ci_worker
package commands

object HelpCommand extends Command:
  override def name: String = "help"

  override def description: String = "Prints this help message"

  override def validateArgs(args: Array[String]) = true

  override def run(args: Array[String]): Unit =
    println("Available commands:")
    Command.commands.foreach { command =>
      println(s"* ${command.name} - ${command.description}\n")
    }
