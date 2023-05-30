package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinTypes.LeftJoin
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Messages.JoinExceptionMessage
import com.bbva.datioamproduct.fdevdatio.common.fields._
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

package object transformationsCourse {
  case class ReplaceColumnException(message: String, columnName: String, columns: Array[String])
    extends Exception(message)

  case class JoinException(expectedKeys: Set[String],
                           colums: Array[String],
                           location: String = "com.bbva.datioamproduct.fdevdatio.MapToDataFrame.getFullDF",
                           message: String
                          )
    extends Exception(message)

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit class CustomTransformations(df: DataFrame) {

    def getMinContractYear: Int = {
      df.rdd
        .map(row => row.getInt(6))
        .reduce((a, b) => if (a > b) b else a)
    }

    def filterMinContractYear: DataFrame = {
      val minContractYear: Int = df.getMinContractYear
      df.filter(col("club_contract_valid_until") === minContractYear)
    }

    def filterNationalityId(nationalityId: Int): DataFrame = {
      if (nationalityId == -1) {
        df
      } else {
        df.filter(NationalityId.column === nationalityId)
      }
    }

    def filterLeague(leagueName: String): DataFrame = {
      df.filter(LeagueName.column === leagueName)
    }

    @throws[Exception]
    def addColumn(newColumn: Column): DataFrame = {
      try {
        val columns: Array[Column] = df.columns.map(col) :+ newColumn
        df.select(columns: _*)
      } catch {
        case exception: Exception => throw exception
      }
    }

    def addColumns(columns: Column*): DataFrame = {
      columns.toList match {
        case head :: tail => df.select(df.columns.map(col) :+ head: _*).addColumns(tail: _*)
        case _ => df
      }
    }

    @throws[ReplaceColumnException]
    def replaceColumn(field: Column): DataFrame = {

      val columnName: String = field.expr.asInstanceOf[NamedExpression].name

      if (df.columns.contains(columnName)) {
        val columns: Array[Column] = df.columns.map {
          case name: String if name == columnName => field
          case _@name => col(name)
        }
        df.select(columns: _*)
      } else {
        val message: String = s"La columna $columnName no puede ser reemplazada."
        throw ReplaceColumnException(message, columnName, df.columns)
      }
    }

    def aggregateExample: DataFrame = {
      df
        .groupBy(NationalityName.column)
        .agg(OverallByNationality(), PlayersByNationality())
        .orderBy(PlayersByNationality.column.desc)
    }


    def aggregatePivotExample: DataFrame = {
      df
        .groupBy(LeagueName.column)
        .pivot(PlayerTraitsExploded.column, Seq("Leadership", "EarlyCrosser", "PowerHeader", "PowerFree-Kick"))
        .agg(
          count("*") alias "count"
        )
        .filter(LeagueName.column isin("Argentina Primera División", "USA Major League Soccer", "Mexican Liga MX"))
    }

    def test: DataFrame = {

      val columns: Array[Column] = df.schema.fields.map(field => {
        field.dataType match {
          case StringType => ???
          case _ => col(field.name)
        }
      })

      df.select(columns: _*)
    }


    def exercise8: DataFrame = {
      val data = df.rdd.map { row: Row =>
        val key: String = row.getString(1)
        val firstName: String = row.getString(2)

        key match {
          case "000" => Row(s"key:${key} Name:${firstName} Matched")
          case _ => Row("unmatched")
        }
      }

      val schema: StructType = StructType(
        Seq(
          StructField("value", StringType)
        )
      )

      val spark: SparkSession = DatioSparkSession.getOrCreate().getSparkSession
      spark.createDataFrame(data, schema)
    }

  }


  implicit class MapToDataFrame(dfMap: Map[String, DataFrame]) {

    @throws[JoinException]
    def getFullDF: DataFrame = {

      val playersKeys: Set[String] = Set(SofifaId.name, ClubTeamId.name, NationTeamId.name)
      val clubPlayersKeys: Set[String] = Set(SofifaId.name, ClubTeamId.name)
      val nationalPlayersKeys: Set[String] = Set(SofifaId.name, NationTeamId.name)
      val nationalitiesKeys: Set[String] = Set(NationalityId.name)

      if (!playersKeys.subsetOf(dfMap(PlayersTag).columns.toSet)) {
        throw JoinException(
          expectedKeys = playersKeys,
          colums = dfMap(PlayersTag).columns,
          message = JoinExceptionMessage(PlayersTag)
        )
      } else if (!clubPlayersKeys.subsetOf(dfMap(ClubPlayersTag).columns.toSet)) {
        throw JoinException(
          expectedKeys = clubPlayersKeys,
          colums = dfMap(ClubPlayersTag).columns,
          message = JoinExceptionMessage(ClubPlayersTag)
        )
      }
      else if (!nationalPlayersKeys.subsetOf(dfMap(NationalPLayersTag).columns.toSet)) {
        throw JoinException(
          expectedKeys = nationalPlayersKeys,
          colums = dfMap(NationalPLayersTag).columns,
          message = JoinExceptionMessage(NationalPLayersTag)
        )
      } else if (!nationalitiesKeys.subsetOf(dfMap(NationalitiesTag).columns.toSet)) {
        throw JoinException(
          expectedKeys = nationalitiesKeys,
          colums = dfMap(NationalitiesTag).columns,
          message = JoinExceptionMessage(NationalitiesTag)
        )
      } else {
        dfMap(PlayersTag)
          .join(dfMap(ClubPlayersTag), Seq(SofifaId.name, ClubTeamId.name), LeftJoin)
          .join(dfMap(ClubTeamsTag), Seq(ClubTeamId.name), LeftJoin)
          .join(dfMap(NationalPLayersTag), Seq(NationTeamId.name, SofifaId.name), LeftJoin)
          .join(dfMap(NationalitiesTag), Seq(NationalityId.name), LeftJoin)
      }
    }
  }
}
