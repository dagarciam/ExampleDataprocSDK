package com.bbva.datioamproduct.fdevdatio.steps

import com.bbva.datioamproduct.fdevdatio.utils.Common
import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.Matchers

import java.util
import scala.annotation.tailrec

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

  import scala.collection.JavaConverters._
  import java.util.Map

  Then("""^(\S+) dataframe has the next values in column (\S+):""") {
    (dfName: String, columnName: String, dataTable: DataTable) => {

      val data: List[Map[String, String]] = dataTable.asMaps().asScala.toList
      val values: Seq[Int] = data.map(_.get("value").toInt)

      Common.dfMap(dfName).filter(!col(columnName).isin(values: _*)).count() shouldBe 0

    }
  }

  Then("""^...$""") {
    (dfName: String, columnCompare: String, dataTable: DataTable) => {
      val data: List[util.Map[String, String]] = dataTable.asMaps().asScala.toList
      val columns: Seq[String] = data.map(_.get("columns").toString)

      val sumColumns: Column = columns.foldLeft(lit(0.0))((a, b) => a + col(b))

      Common.dfMap(dfName)
        .withColumn("test", col(columnCompare) - sumColumns)
        .filter(col("test") =!= 0.0)
        .count() shouldBe 0
    }
  }

  Then("""^(\S+) dataframe has the columns calculated with the next arithmetic operations:$""") {
    (dfName: String, dataTable: DataTable) => {
      val data: List[util.Map[String, String]] = dataTable.asMaps().asScala.toList
      val rules: collection.Map[String, String] = data.map(row => row.get("column") -> row.get("operation")).toMap
      val df: DataFrame = Common.dfMap(dfName)

      rules.foreach {
        case (column, operation) =>
          df.filter(s"$column <> ($operation)").count() shouldBe 0
      }
    }
  }

}
