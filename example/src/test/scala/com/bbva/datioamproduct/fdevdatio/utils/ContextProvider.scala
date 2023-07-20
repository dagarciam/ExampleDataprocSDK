package com.bbva.datioamproduct.fdevdatio.utils

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}

import java.util.Locale

trait ContextProvider extends FlatSpec with BeforeAndAfterAll with Matchers with FakeDataCatalog {
  self: Suite =>

  @transient implicit var datioSparkSession: DatioSparkSession = _

  @transient var sparkContext: SparkContext = _

  @transient var sqlContext: SQLContext = _

  val config: Config = new ProcessConfigLoader().fromPath("src/test/resources/config/application-test.conf")

  private val sparkWarehouseDir: String = s"${System.getProperty("user.dir")}/src/test/resources/warehouse"

  override def beforeAll(): Unit = {
    super.beforeAll()

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

    datioSparkSession = DatioSparkSession.getOrCreate()

    sparkContext = datioSparkSession.getSparkSession.sparkContext

    sqlContext = datioSparkSession.getSparkSession.sqlContext

    createDataCatalog()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    if (datioSparkSession.getSparkSession != null) {
      datioSparkSession.getSparkSession.stop()
    }
  }

}