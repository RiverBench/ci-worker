package io.github.riverbench.ci_worker
package util

import com.typesafe.config.ConfigFactory

object AppConfig:
  private var config = ConfigFactory.load()

  /**
   * ONLY FOR USE IN TESTS. Sets a new base config to use by the Semantic Repository.
   *
   * @param newConfig new config
   */
  def setNewConfig(newConfig: com.typesafe.config.Config): Unit =
    config = newConfig

  /**
   * @return base config
   */
  def getConfig = config

  object CiWorker:
    val baseDownloadUrl = config.getString("ci-worker.base-download-url")
