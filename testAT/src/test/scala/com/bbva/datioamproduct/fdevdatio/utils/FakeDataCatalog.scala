package com.bbva.datioamproduct.fdevdatio.utils

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.util.Locale

trait FakeDataCatalog {
  private val sparkWarehouseDir: String = "src/test/resources/warehouse"

  def createDataCatalog()(implicit datioSparkSession: DatioSparkSession): Unit = {
    Locale.setDefault(new Locale("CO"))

    datioSparkSession.getSparkSession.sql("CREATE DATABASE IF NOT EXISTS input")
    datioSparkSession.getSparkSession.sql("CREATE DATABASE IF NOT EXISTS output")
    createTable(
      s"$sparkWarehouseDir/schema/t_fdev_customers.output.schema",
      "t_fdev_customers",
      partitions = List("gl_date")
    )
    createTable(
      s"$sparkWarehouseDir/schema/t_fdev_phones.output.schema",
      "t_fdev_phones",
      partitions = List("cutoff_date")
    )
    createTable(
      s"$sparkWarehouseDir/schema/t_fdev_customersphones.output.schema",
      "t_fdev_customersphones",
      partitions = List("jwk_date"),
      db = "output"
    )
  }

  private def createTable(schemaPath: String, tableName: String, partitions: List[String] = Nil, db: String = "input")
                         (implicit datioSparkSession: DatioSparkSession): Unit = {
    val datioSchema = DatioSchema.getBuilder
      .fromURI(new URI(schemaPath))
      .withMetadataFields(true)
      .withDeletedFields(true)
      .build()


    val fieldsDDL: String = datioSchema.getStructType
      .map(_.toDDL).mkString(",")

    val createTableDLL: String = s"CREATE EXTERNAL TABLE IF NOT EXISTS $db.$tableName " +
      s"($fieldsDDL) " +
      s"STORED AS parquet " +
      s"LOCATION '$tableName'"

    partitions match {
      case Nil => datioSparkSession.getSparkSession.sql(createTableDLL)
      case _ =>
        datioSparkSession.getSparkSession.sql(s"$createTableDLL PARTITIONED BY (${partitions.mkString(",")})")
        datioSparkSession.getSparkSession.sql(s"MSCK REPAIR TABLE $db.$tableName")
    }

  }

}
