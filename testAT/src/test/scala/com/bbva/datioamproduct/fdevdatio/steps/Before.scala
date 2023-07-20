package com.bbva.datioamproduct.fdevdatio.steps

import com.bbva.datioamproduct.fdevdatio.utils.FakeDataCatalog
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers

class Before extends ScalaDsl with EN with Matchers with FakeDataCatalog {
  private val sparkWarehouseDir: String = s"${System.getProperty("user.dir")}/src/test/resources/warehouse"

  Before {
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

    createDataCatalog()(DatioSparkSession.getOrCreate())
  }
}
