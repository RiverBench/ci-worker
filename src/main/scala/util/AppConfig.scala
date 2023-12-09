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
    val baseDatasetUrl = config.getString("ci-worker.base-dataset-url")
    val baseProfileUrl = config.getString("ci-worker.base-profile-url")
    val baseTaskUrl = config.getString("ci-worker.base-task-url")
    val baseCategoryUrl = config.getString("ci-worker.base-category-url")
    val rbRootUrl = config.getString("ci-worker.rb-root-url")
