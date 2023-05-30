package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{DevNameTag, InputTag, LeagueNameTag, NationalityIdTag, ODateTag, ParamsTag}
import com.typesafe.config.{Config, ConfigObject}
import org.apache.spark.sql.DataFrame

import scala.collection.convert.ImplicitConversions.`set asScala`
import scala.language.implicitConversions

package object utils {

  case class Params(nationalityId: Int, devName: String, odate: String, leagueName:String)

  implicit class SuperConfig(config: Config) extends IOUtils {
    def readInputs: Map[String, DataFrame] = {
      config.getObject(InputTag).keySet()
        .map((key: String) => {
          val inputConfig: Config = config.getConfig(s"$InputTag.$key")
          (key, read(inputConfig))
        })
        .toMap
    }

    def getParams: Params = Params(
      nationalityId = config.getInt(NationalityIdTag),
      devName = config.getString(DevNameTag),
      odate = config.getString(ODateTag),
      leagueName = config.getString(LeagueNameTag)
    )
  }

}