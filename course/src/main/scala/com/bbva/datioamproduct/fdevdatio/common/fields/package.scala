package com.bbva.datioamproduct.fdevdatio.common

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.AlphabeticalConstants.{A, B, C, Comma, D}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.NumericConstants.Twenty
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

package object fields {


  case object Age extends Field {
    override val name: String = "age"
  }

  case object Overall extends Field {
    override val name: String = "overall"
  }

  case object HeightCm extends Field {
    override val name: String = "height_cm"
  }

  case object SofifaId extends Field {
    override val name: String = "sofifa_id"
  }

  case object ClubTeamId extends Field {
    override val name: String = "club_team_id"
  }

  case object NationalityId extends Field {
    override val name: String = "nationality_id"
  }

  case object NationTeamId extends Field {
    override val name: String = "nation_team_id"
  }

  case object NationalityName extends Field {
    override val name: String = "nationality_name"
  }

  case object ShortName extends Field {
    override val name: String = "short_name"
  }

  case object CatAgeOverall extends Field {
    override val name: String = "cat_age_overall"

    def apply(): Column = {
      when(Age.column <= Twenty || Overall.column > 80, A)
        .when(Age.column <= 23 || Overall.column > 70, B)
        .when(Age.column <= 30, C)
        .otherwise(D) alias name
    }
  }

  case object SumOverall extends Field {
    override val name: String = "sum_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(ClubName.column, CatAgeOverall.column)

      sum(Overall.column).over(w) alias name
    }
  }

  case object CatHeight extends Field {
    override val name: String = "cat_height"

    def apply(): Column = {
      when(HeightCm.column > 200, "A")
        .when(HeightCm.column > 185, "B")
        .when(HeightCm.column > 175, "C")
        .when(HeightCm.column > 165, "D")
        .otherwise("E")
        .alias(name)
    }
  }

  case object ZScore extends Field {
    override val name: String = "z_score"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column, CatAgeOverall.column)

      val stddevOverall: Column = stddev(Overall.column).over(w)
      val meanOverall: Column = mean(Overall.column).over(w)

      ((Overall.column - meanOverall) / stddevOverall) alias name
    }
  }

  object PlayersByNationality extends Field {
    override val name: String = "players_by_nationality"

    def apply(): Column = count(Overall.column) alias name
  }

  object OverallByNationality extends Field {
    override val name: String = "overall_by_nationality"

    def apply(): Column = avg(Overall.column) alias name
  }

  case object LeagueName extends Field {
    override val name: String = "league_name"
  }

  case object ClubName extends Field {
    override val name: String = "club_name"
  }

  case object ClubPosition extends Field {
    override val name: String = "club_position"
  }

  case object ClubContractValidUntil extends Field {
    override val name: String = "club_contract_valid_until"
  }

  case object PlayerPositions extends Field {
    override val name: String = "player_positions"

    def apply(): Column = {
      split(regexp_replace(column, " ", ""), Comma) alias name
    }
  }

  case object ExplodePlayerPositions extends Field {
    override val name: String = "explode_player_positions"

    def apply(): Column = explode(PlayerPositions.column) alias name
  }

  case object PlayerTraits extends Field {
    override val name: String = "player_traits"

    def apply(): Column = {
      split(regexp_replace(column, " ", ""), Comma) alias name
    }
  }

  case object PlayerTraitsExploded extends Field {
    override val name: String = "player_traits_exploded"

    def apply(): Column = explode(PlayerTraits.column) alias name
  }

  case object CountByPlayerPositions extends Field {
    override val name: String = "count_by_position"

    def apply(): Column = count(ExplodePlayerPositions.column) alias name
  }

  case object StdDevOverall extends Field {
    override val name: String = "std_dev_overall"

    def apply(): Column = {

      val w: WindowSpec = Window.partitionBy(NationalityName.column)

      stddev(Overall.column).over(w).alias(name)
    }

  }

  case object MeanOverall extends Field {
    override val name: String = "mean_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column)
      mean(Overall.column).over(w).alias(name)
    }
  }

  case object Passing extends Field {
    override val name: String = "passing"
  }

  case object AgegRN extends Field {
    override val name: String = "age_rn"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
      row_number().over(w).alias(name)
    }
  }

  case object AgeRank extends Field {
    override val name: String = "age_rank"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
      rank().over(w).alias(name)
    }
  }

  case object AgeDenseRank extends Field {
    override val name: String = "age_dense_rank"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
      dense_rank().over(w).alias(name)
    }
  }

  case object ShortNameLead extends Field {
    override val name: String = "short_name_lead"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
      lead(col("short_name"), 1).over(w).alias(name)
    }
  }

  case object ShortNameLag extends Field {
    override val name: String = "short_name_lag"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column).orderBy(Age.column)
      lag(col("short_name"), 1).over(w).alias(name)
    }
  }


  case object CutoffDate extends Field {
    override val name = "cutoff_date"

    def apply(cutoffDate: Int): Column = {
      lit(cutoffDate) alias name
    }
  }


  case object Odate extends Field {
    override val name: String = "odate"

    def apply(odate: String): Column = {
      val lastDay: Column = last_day(to_date(lit(odate), "yyyyMM"))
      lastDay alias name
    }
  }
}
