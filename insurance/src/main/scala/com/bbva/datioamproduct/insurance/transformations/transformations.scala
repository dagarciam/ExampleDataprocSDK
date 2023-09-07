package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.AlfanumericConstanst.Rojo
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinTypes.{Inner, LeftJoin}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.MessageExpections.JoinExceptionColumns
import com.bbva.datioamproduct.fdevdatio.common.fields._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, when}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.{functions => f}



package object  transformations {

  case class ReplaceColumnException(message: String, columnName: String, column: Array[String])
    extends Exception(message)

  implicit class CustomTransformations(df: DataFrame) {

    def addColumn(newColumn: Column): DataFrame = {
      try{
        val columns: Array[Column] = df.columns.map(col) :+ newColumn

        df.select(columns: _*)
      }catch {
        case exception: Exception => throw exception
      }
    }

    def updateIncidents: DataFrame = {
      df.withColumn(IncidentsId.name,
        concat(IdPolicy.column.substr(10,4),
          Clasification.column))
    }


    def magicMethod: DataFrame = {
      df.select(
        df.columns.map {
          case name: String if name == "club_jersey_number" => lit(0) alias name
          case _@name => col(name)
        }: _*
      )
    }

    @throws[ReplaceColumnException]
    def replaceColumn(field: Column): DataFrame = {
      val columnName: String = field.expr.asInstanceOf[NamedExpression].name
      if(df.columns.contains(columnName)) {
        val columns: Array[Column] = df.columns.map {
          case name: String if name == columnName => field
          case _@name => col(name)
        }

        df.select(columns: _*)
      } else {
        val message: String = s"La columna $columnName no puede ser remplazada."
        throw ReplaceColumnException(message, columnName, df.columns)
      }

    }

  }


  case class JoinException (expectedKeys: Array[String],
                            columns: Array[String],
                            location: String = "com.bbva.datioamproduct.fdevdatio.MapToDataFrame.getFullDF",
                            message: String) extends Exception(message)

  implicit class MapToDataFrame(dfMap: Map[String,DataFrame]) {

    @throws[JoinException]
    def getFullDF(): DataFrame = {
      val dfPolicy : DataFrame = dfMap("t_mdit_policy").distinct()
      val dfIncident : DataFrame = dfMap("t_mdit_incidents")
      val dfClient : DataFrame = dfMap("t_mdit_clientes").distinct()
      val dfCar : DataFrame = dfMap("t_mdit_cars")

      println(dfClient.count())

      dfPolicy
        .join(dfClient, f.lower(dfPolicy("id_client")) === dfClient("user_id"),LeftJoin)
        .join(dfIncident,dfPolicy("id_policy") === dfIncident("policy_id"),LeftJoin)
        .join(dfCar,f.upper(dfPolicy("id_car_brand")) === dfCar("brand_name"),LeftJoin)

    }
  }

}
