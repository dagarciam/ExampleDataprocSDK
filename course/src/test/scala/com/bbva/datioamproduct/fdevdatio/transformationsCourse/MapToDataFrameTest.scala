package com.bbva.datioamproduct.fdevdatio.transformationsCourse

import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.utils.SuperConfig
import org.apache.spark.sql.DataFrame

class MapToDataFrameTest extends ContextProvider {

  "getFullDF method" should "return a DF with 76 columns" in {

    val dfMap: Map[String, DataFrame] = config.readInputs

    val outputDF: DataFrame = dfMap.getFullDF

    outputDF.columns.length shouldBe 76

  }
}
