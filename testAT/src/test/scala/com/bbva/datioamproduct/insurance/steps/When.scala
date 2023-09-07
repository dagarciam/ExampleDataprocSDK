package com.bbva.datioamproduct.insurance.steps

import com.bbva.datioamproduct.insurance.utils.Common
import com.datio.dataproc.sdk.launcher.SparkLauncher
import io.cucumber.scala.{EN, ScalaDsl}
import org.scalatest.Matchers

class When extends ScalaDsl with EN with Matchers {
  When("""^I execute the process: (.*)$""") {
    (processId: String) => {
      println(Common.configPath)
      println(processId)
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
