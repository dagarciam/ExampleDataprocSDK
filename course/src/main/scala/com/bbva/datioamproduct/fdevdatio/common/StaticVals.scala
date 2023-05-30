package com.bbva.datioamproduct.fdevdatio.common

object StaticVals {

  case class Player(name: String, overall: Int) extends Serializable

  case object CourseConfigConstants {
    val RootTag: String = "courseJob"
    val InputTag: String = s"$RootTag.input"
    val OutputTag: String = s"$RootTag.output"
    val ParamsTag: String = s"$RootTag.params"

    val NationalityIdTag: String = s"$ParamsTag.nationalityId"
    val DevNameTag: String = s"$ParamsTag.devName"
    val ODateTag: String = s"$ParamsTag.oDate"
    val LeagueNameTag: String = s"$ParamsTag.leagueName"

    val Fifa22Tag: String = s"$OutputTag.fdevFifa22"

    val ClubPlayersTag: String = "fdevClubPlayers"
    val NationalitiesTag: String = "fdevNationalities"
    val NationalTeamsTag: String = "fdevNationalTeams"
    val NationalPLayersTag: String = "fdevNationalPlayers"
    val PlayersTag: String = "fdevPlayers"
    val ClubTeamsTag: String = "fdevClubTeams"

  }

  case object NumericConstants {
    val NegativeOne: Int = -1
    val Zero: Int = 0
    val Twenty: Int = 20

  }

  case object AlphabeticalConstants {
    val Comma: String = ","
    val A: Char = 'A'
    val B: Char = 'B'
    val C: Char = 'C'
    val D: Char = 'D'
  }

  case object Messages {

    val WelcomeMessage: String => String =
      (devName: String) => s"Bienvenido al curso de procesamiento: $devName"

    val JoinExceptionMessage: String => String =
      (dfTag: String) => s"No se encontraron las columnas llave necesarias para el join en el DF: $dfTag"

  }

  case object JoinTypes {
    val LeftJoin: String = "left"
    val LeftAntiJoin: String = "left_anti"
    val LeftSemiJoin: String = "left_semi"
  }
}
