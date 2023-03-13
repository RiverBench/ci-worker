package io.github.riverbench.ci_worker

import commands.Command

@main
def main(args: String*): Unit =
  Command.runCommand(args.toArray)

