package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{Fifa22Tag, NationalityIdTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Messages.WelcomeMessage
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.NumericConstants.{NegativeOne, Zero}
import com.bbva.datioamproduct.fdevdatio.common.fields.{ClubName, NationalityName, PlayerPositions, PlayerTraits, ShortName}
import com.bbva.datioamproduct.fdevdatio.transformationsCourse._
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.bbva.datioamproduct.fdevdatio.utils.{Params, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, NamedExpression}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class CourseSparkProcess extends SparkProcess with IOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {

    Try {
      val config: Config = runtimeContext.getConfig
      val params: Params = config.getParams

      logger.info(WelcomeMessage(params.devName))
      val dfMap: Map[String, DataFrame] = config.readInputs

      val fullDF: DataFrame = dfMap.getFullDF
        .filterNationalityId(config.getInt(NationalityIdTag))
        .replaceColumn(PlayerPositions())
        .replaceColumn(PlayerTraits())

      write(fullDF, config.getConfig(Fifa22Tag))


    } match {
      case Success(_) => Zero
      case Failure(exception: ReplaceColumnException) =>
        logger.error(exception.getMessage)
        logger.error(s"Columna a reeplazar: ${exception.columnName}")
        logger.error(s"Columnas encontradas: [${exception.columns.mkString(", ")}]")
        NegativeOne
      case Failure(exception: JoinException) =>
        logger.error(exception.getMessage)
        logger.error(s"Expected key columns: [${exception.expectedKeys.mkString(", ")}]")
        logger.error(s"Found columns in DF: [${exception.colums.mkString(", ")}]")
        logger.error(s"Error produced in method: ${exception.location}")
        NegativeOne
      case Failure(exception: InvalidDatasetException) =>
        exception.getErrors.forEach { error =>
          logger.error(error.toString)
        }
        NegativeOne
      case Failure(exception: Exception) =>
        exception.printStackTrace()
        NegativeOne
    }
  }

  override def getProcessId: String = "CourseSparkProcess"

}
