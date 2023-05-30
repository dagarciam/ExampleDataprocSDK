package com.bbva.datioamproduct.fdevdatio.testUtils.dummies

import com.bbva.datioamproduct.fdevdatio.common.fields.NationalityId
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object DummyPlayersDF {

  private val data: Seq[Row] = Seq(
    Row(83),
    Row(83),
    Row(81),
    Row(86),
    Row(10)
  )

  private val schema: StructType = StructType(
    Seq(
      StructField(NationalityId.name, IntegerType)
    )
  )

  def getDF(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

}
