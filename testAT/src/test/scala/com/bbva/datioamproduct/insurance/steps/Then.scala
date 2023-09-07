package com.bbva.datioamproduct.insurance.steps

import com.bbva.datioamproduct.insurance.utils.Common
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.col
import org.scalatest.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.Map
import scala.collection.JavaConverters.asScalaBufferConverter

class Then extends ScalaDsl with EN with Matchers {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

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

  Then("""^(\S+) dataframe does not have null values for columns:$"""){
    (dfName: String, dataTable: DataTable) =>
      val COLUMN_NAME: String = "Column"
      val data: List[Map[String,String]] = dataTable.asMaps().asScala.toList
      val column: Set[String] = data.map(_.get(COLUMN_NAME)).toSet

      var fail = false

      column foreach(colum => {
        val nullCount:Long =Common.dfMap(dfName).filter(col(colum).isNull ||
        col(colum).equalTo("")).count()
        logger.info(s"Null values for $dfName dataFrame in column $colum : $nullCount")
        fail = fail || (nullCount > 0)
      })
      Common.dfMap(dfName).show()

      withClue(s"Some columns of $dfName dataframe have null values .") {
        fail shouldBe false
      }

  }
}
