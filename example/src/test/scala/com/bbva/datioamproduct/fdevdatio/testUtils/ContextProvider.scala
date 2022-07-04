package com.bbva.datioamproduct.fdevdatio.testUtils

import com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}

trait ContextProvider extends FlatSpec with BeforeAndAfterAll with Matchers {
  self: Suite =>

  @transient var spark: SparkSession = _

  @transient var sparkContext: SparkContext = _

  @transient var sqlContext: SQLContext = _

  val config: Config = new ProcessConfigLoader().fromPath("src/test/resources/config/application-test.conf")

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("spark session")
      .master("local[*]")
      .getOrCreate()

    sparkContext = spark.sparkContext

    sqlContext = spark.sqlContext

  }

  override def afterAll(): Unit = {
    super.afterAll()

    if (spark != null) {
      spark.stop()
    }
  }
}