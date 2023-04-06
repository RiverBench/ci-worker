package io.github.riverbench.ci_worker
package commands

import scala.concurrent.Future

object ErrorCommand extends Command:
  override def name = "INVALID"

  override def description = "INVALID"

  override def validateArgs(args: Array[String]) = true

  override def run(args: Array[String]): Future[Unit] =
    Future {
      args.headOption match
        case None => println("No command was specified. Use 'help' to see a list of commands")
        case Some(cmd) => println(s"Command $cmd is not valid. Use 'help' to see a list of commands")
    }
