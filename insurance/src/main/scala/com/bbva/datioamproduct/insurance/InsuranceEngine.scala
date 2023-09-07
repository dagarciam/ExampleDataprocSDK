package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{DevNameTag, InsuranceTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Messages.WelcomeMessage
import com.bbva.datioamproduct.fdevdatio.common.fields._
import com.bbva.datioamproduct.fdevdatio.transformations._
import com.bbva.datioamproduct.fdevdatio.utils.{CourseJobConfig, IOUtils, Params}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.{functions => f}


import scala.util.{Failure, Success, Try}

class InsuranceEngine extends SparkProcess with IOUtils{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("\n------Initializing processing . \n .. \n ...")

    Try{
      val config: Config = runtimeContext.getConfig
      val params: Params = config.getParams

      val devName: String = config.getString(DevNameTag)
      logger.info(WelcomeMessage(params.devName))
      val dfMap: Map[String, DataFrame] = config.readInputs
      val dfFullPolicy: DataFrame = dfMap.getFullDF()

      val resultDf:DataFrame = dfFullPolicy
        .addColumn(Clasification())
        .updateIncidents.select(f.upper(IdPolicy.column)
        ,f.upper(CompleteName.column),f.upper(IdCardBrand.column.cast(StringType))
        ,f.upper(IncidentsId.column),f.upper(CarColor.column)
        ,f.upper(RepairAmount.column),f.upper(CarModel.column)
        ,f.upper(CurrentPayment.column),f.upper(BrandCar.column))

      resultDf.show(300)

      //write(resultDf,config.getConfig(InsuranceTag))

    } match {
      case Success(_) => 0
      case Failure(exception: InvalidDatasetException) =>
        for (err <- exception.getErrors.toArray) {
          logger.error(err.toString)
        }
        -1
      case Failure(exception: Exception) =>
        exception.printStackTrace()
        -1
    }
  }
  override def getProcessId: String = "InsuranceEngine"
}