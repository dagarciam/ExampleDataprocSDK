package com.bbva.datioamproduct.fdevdatio.common

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.AlfanumericConstanst.Rojo
import org.apache.spark.sql.{Column, expressions}
import org.apache.spark.sql.functions.{when}

package object fields {

  //Case object que funciona como un inicializador de la variable name
  //Este al igual que los demas hereda razgos del traid Field.

  case object VideogameGener extends Field {
    override val name: String = "videgoame_genre"
  }

  case object CompleteName extends Field {
    override val name: String = "complete_name"
  }

  case object IdCardBrand extends Field {
    override val name: String = "brand_car_id"
  }

  case object RepairAmount extends Field {
    override val name: String = "repair_amount"
  }

  case object CarModel extends Field {
    override val name: String = "car_model"
  }

  case object BrandCar extends Field {
    override val name: String = "brand_name"
  }

  case object CurrentPayment extends Field {
    override val name: String = "current_payment"
  }

  case object Age extends Field {
    override val name: String = "user_age"
  }

  case object VelocityMax extends Field {
    override val name: String = "velocity_max_amount"
  }

  case object CarColor extends Field {
    override val name: String = "car_color"
  }

  case object IncidentsId extends Field {
    override val name: String = "incidents_id"
  }

  case object IdPolicy extends Field {
    override val name: String = "id_policy"
  }

  case object Clasification extends Field {
    override val name: String = "clasification"

    def apply(): Column = {
      when(VelocityMax.column.substr(0, 3) >= 200 && Age.column < 25 && CarColor.column.equalTo(Rojo),
        "A")
        .when(VelocityMax.column.substr(0, 3) >= 200 && Age.column >= 25 && CarColor.column.equalTo(Rojo),
          "C")
        .when(VelocityMax.column.substr(0, 3) < 200 && Age.column < 25 && CarColor.column.equalTo(Rojo),
          "B")
        .when(VelocityMax.column.substr(0, 3) >= 200 && Age.column < 25 && CarColor.column.notEqual(Rojo),
          "A")
        .when(VelocityMax.column.substr(0, 3) >= 200 && Age.column >= 25 && CarColor.column.notEqual(Rojo),
          "C")
        .when(VelocityMax.column.substr(0, 3) < 200 && Age.column < 25 && CarColor.column.notEqual(Rojo),
          "C")
        .when(VelocityMax.column.substr(0, 3) < 200 && Age.column >= 25 && CarColor.column.equalTo(Rojo),
          "C").otherwise("C") alias(name)
    }
  }

}