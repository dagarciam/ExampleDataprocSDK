package com.bbva.datioamproduct.fdevdatio.utilsCourse

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.Fifa22Tag
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.transformationsCourse.MapToDataFrame
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, SuperConfig}
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import org.apache.spark.sql.DataFrame

class IOUtilsTest extends ContextProvider {

  "write method" should "throw a InvalidDatasetException" in {
    case object IOUtilsTest extends IOUtils

    val inputDF: DataFrame = config.readInputs.getFullDF

    assertThrows[InvalidDatasetException] {
      IOUtilsTest.write(inputDF, config.getConfig(Fifa22Tag))
    }
  }

  /*"write method" should "finish a succeed execution" in {
    val inputDF: DataFrame = config.readInputs
      .getFullDF

    write(inputDF, config.getConfig(Fifa22Tag))

    succeed
  }*/


}
