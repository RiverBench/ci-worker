package io.github.riverbench.ci_worker

import commands.Command

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Global:
  val actorSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "ci-worker")

@main
def main(args: String*): Unit =
  implicit val ec: ExecutionContext = Global.actorSystem.executionContext

  val commandFuture = Command.runCommand(args.toArray)
  commandFuture.onComplete {
    case Success(_) =>
      System.exit(0)
    case Failure(e) =>
      e.printStackTrace()
      System.exit(1)
  }
