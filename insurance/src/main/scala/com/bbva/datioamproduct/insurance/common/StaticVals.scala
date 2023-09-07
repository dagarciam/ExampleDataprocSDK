package com.bbva.datioamproduct.fdevdatio.common

/*
* Dentro de este .scala lo que se hace es poner aquellos valores
* que son constantes o estaticos pero no contiene valores muy grandes o mensajes de logs
* pueden definirse varios case objects
* */
object StaticVals {
  case object CourseConfigConstants {
    val RootTag:String = "insuranceJob"
    val InputTag: String = s"$RootTag.input"
    val OutputTag: String = s"$RootTag.output"
    val InsuranceTag: String = s"$OutputTag.t_mdit_insurance"
    val DevNameTag: String = s"$RootTag.params.devName"
  }
  case object NumericsConstants{
    val Zero = 0
  }

  case object AlfanumericConstanst{
    val Comma = ","
    val Rojo = "rojo"
  }

  case object Messages {
    val WelcomeMessage: String => String = {
      (devName: String) => s"-----------------------$devName-------------------------------"
    }
  }

  case object JoinTypes {
    val LeftJoin: String = "left"
    val OuterJoin: String = "outer"
    val Inner: String = "inner"
    val LeftAnti: String = "leftanti"
  }

  case object MessageExpections {
    val JoinExceptionColumns: Array[String] => String ={ (columns: Array[String]) => s"Los campos [${columns.mkString(", ")}] no se encontraron"}
  }

}
