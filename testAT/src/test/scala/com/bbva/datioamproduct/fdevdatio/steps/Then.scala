package com.bbva.datioamproduct.fdevdatio.steps

import com.bbva.datioamproduct.fdevdatio.utils.Common
import io.cucumber.scala.{EN, ScalaDsl}
import org.scalatest.Matchers

class Then extends ScalaDsl with EN with Matchers {
  Then("""^the exit code should be (\d)$""") {
    (exitCode: Int) => Common.exitCode should be(exitCode)
  }

  Then("""^(\S+) dataframe has exactly (\d+) records$""") {
    (dfName: String, records: Int) => {
      Common.dfMap(dfName).count() should be(records)
    }
  }

  Then("""^(\S+) has exactly the same number of records than (\S+)$""") {
    (dfName: String, dfName2: String) => {
      Common.dfMap(dfName).count() should be(Common.dfMap(dfName2).count())
    }
  }
}
