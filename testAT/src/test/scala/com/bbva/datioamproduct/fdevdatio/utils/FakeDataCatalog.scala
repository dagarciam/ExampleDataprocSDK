package com.bbva.datioamproduct.fdevdatio.utils

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.util.Locale

trait FakeDataCatalog {
  private val sparkWarehouseDir: String = s"${System.getProperty("user.dir")}/src/test/resources/warehouse"
  SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.warehouse.dir", sparkWarehouseDir)
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
    .enableHiveSupport()
    .getOrCreate()

  lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()

  def createDataCatalog(): Unit = {
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

  private def createTable(schemaPath: String, tableName: String, partitions: List[String] = Nil, db: String = "input"): Unit = {

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
