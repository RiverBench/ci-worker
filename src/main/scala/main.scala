package io.github.riverbench.ci_worker

import commands.Command

import org.apache.jena.rfc3986.{Issue, Severity, SeverityMap, Violations}
import org.apache.jena.sys.JenaSystem
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Global:
  val actorSystem: ActorSystem[?] = ActorSystem(Behaviors.empty, "ci-worker")

@main
def main(args: String*): Unit =
  implicit val ec: ExecutionContext = Global.actorSystem.executionContext

  // Jena 6 warns about IRIs with lowercase percent-encoding (e.g., %e7), which are valid, but not
  // in the normal form. We parse with strict error handlers that treat warnings as errors, and
  // some of the published datasets have such IRIs, so this check is turned off.
  // Jena must be initialized first, as it sets its own severity map on init.
  JenaSystem.init()
  val severities = java.util.EnumMap[Issue, Severity](Violations.severities().asMap())
  severities.put(Issue.iri_percent_not_uppercase, Severity.IGNORE)
  Violations.setSystemSeverityMap(SeverityMap.create("ci-worker", severities))

  val commandFuture = Command.runCommand(args.toArray)
  commandFuture.onComplete {
    case Success(_) =>
      System.exit(0)
    case Failure(e) =>
      e.printStackTrace()
      System.exit(1)
  }
