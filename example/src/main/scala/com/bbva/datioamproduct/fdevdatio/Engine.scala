package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.ExampleConfigConstants
import com.bbva.datioamproduct.fdevdatio.common.example.StaticVals.JoinTypes
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customers._
import com.bbva.datioamproduct.fdevdatio.common.namings.output.CustomersPhones._
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations._
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class Engine extends SparkProcess with IOUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val OK: Int = 0
  val ERR: Int = -1

  override def runProcess(runtimeContext: RuntimeContext): Int = Try {
    logger.info(s"Process Id: ${runtimeContext.getProcessId}")
    val config: Config = runtimeContext.getConfig

    val jwkDate: String = config.getString(ExampleConfigConstants.JwkDate)

    //Load inputs
    val phonesConfig: Config = config.getConfig(ExampleConfigConstants.PhonesConfig)
    val phonesDF: DataFrame = read(phonesConfig)
    val customersConfig: Config = config.getConfig(ExampleConfigConstants.CustomersConfig)
    val customersDF: DataFrame = read(customersConfig)

    // Regla 1, 2, 3
    val customerPhonesDF: CustomersPhonesTransformer = phonesDF.filterPhones().join(
      customersDF.filterCustomers(),
      Seq(CustomerId.name, DeliveryId.name),
      JoinTypes.INNER
    )

    val outputDF: DataFrame = customerPhonesDF
      .addColumn(CustomerVip()) //Regla 4
      .addColumn(ExtraDiscount()) //Regla 5
      .addColumn(FinalPrice()) // Regla 6
      .addColumn(Age()) //Regla 7
      .filterBrandsTop() //Regla 8
      .addColumn(JwkDate(jwkDate)) //Regla 9
      .cleanNfcColumn() //Regla 10
      .fitToSchema() // Selecciona únicamente las columnas que el esquema indica

    //Writing output (read conf file format)
    val customersPhonesConfig: Config = config.getConfig(ExampleConfigConstants.CustomersPhonesConfig)
    write(outputDF, customersPhonesConfig)

  } match {
    case Success(_) => OK
    case Failure(exception: InvalidDatasetException) =>
      for (err <- exception.getErrors.toArray) {
        logger.error(err.toString)
      }
      ERR
    case Failure(exception: Exception) =>
      exception.printStackTrace()
      ERR

  }

  override def getProcessId: String = "Engine"
}