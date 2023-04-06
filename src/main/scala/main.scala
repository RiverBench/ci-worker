package io.github.riverbench.ci_worker

import commands.Command

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors

object Global:
  val actorSystem: ActorSystem[_] = ActorSystem(Behaviors.empty, "ci-worker")

@main
def main(args: String*): Unit =
  Command.runCommand(args.toArray)
  System.exit(0)

