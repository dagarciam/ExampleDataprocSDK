package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}

class CourseSparkProcess extends SparkProcess with IOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("Hola mundo, desde mi nuevo modulo de dataproc sdk")
    val config: Config = runtimeContext.getConfig

    val playersInputConfig: Config = config.getConfig("courseJob.input.fdevPlayers")
    val playersDF: Dataset[Row] = read(playersInputConfig)

    playersDF.printSchema()
    playersDF.show(false)

    0
  }


  override def getProcessId: String = "CourseSparkProcess"

}
