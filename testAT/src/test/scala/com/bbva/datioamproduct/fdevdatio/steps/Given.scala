package com.bbva.datioamproduct.fdevdatio.steps

import io.cucumber.scala.{EN, ScalaDsl}
import org.scalatest.Matchers
import com.bbva.datioamproduct.fdevdatio.utils.Common
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import org.apache.spark.sql.DataFrame

class Given extends ScalaDsl with EN with Matchers {
  lazy val datioSpark: DatioSparkSession = DatioSparkSession.getOrCreate()
  Given("""^a config file with path: (.*)$""") {
    path: String => {
      Common.configPath = path
    }
  }

  Given("""^a dataframe (\S+) in path: (.*)$""") {
    (dfName: String, path: String) => {
      val df: DataFrame = datioSpark.read().parquet(path)
      Common.dfMap.put(dfName, df)
    }
  }
}
