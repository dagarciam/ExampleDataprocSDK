package com.bbva.datioamproduct.fdevdatio

import com.datio.dataproc.sdk.launcher.SparkLauncher
import org.slf4j.{Logger, LoggerFactory}


object Launcher {
  private val logger:Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Launch your implementation of SparkProcess
   *
   * @param args the only needed argument is the path to the configuration file
   */
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error("Parameter configuration file path is mandatory. Exiting...")
      System.exit(1000)
    }
    SparkLauncher.main(Array(args(0), "Engine"))
  }
}
