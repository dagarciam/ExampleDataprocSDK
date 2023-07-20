package com.bbva.datioamproduct.fdevdatio.steps

import com.bbva.datioamproduct.fdevdatio.utils.{Common, FakeDataCatalog}
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.launcher.SparkLauncher
import com.datio.dataproc.sdk.schema.DatioSchema
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers

import java.net.URI
import java.util.Locale


class When extends ScalaDsl with EN with Matchers with FakeDataCatalog {
  Before{
    createDataCatalog()
  }
  When("""^I execute the process: (.*)$""") {
    (processId: String) => {
      val args = Array(Common.configPath, processId)
      Common.exitCode = new SparkLauncher().execute(args)
    }
  }

  When("""^I filter (\S+) dataframe with the filter: (.*) and save it as (\S+) dataframe$""") {
    (dfName: String, expresionFilter: String, filteredName: String) => {
      Common.dfMap.put(filteredName,
        Common.dfMap(dfName).filter(expresionFilter)
      )
    }
  }

}
