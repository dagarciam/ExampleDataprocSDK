package com.bbva.datioamproduct.fdevdatio.transformationsCourse

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.PlayersTag
import com.bbva.datioamproduct.fdevdatio.common.fields.NationalityId
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.testUtils.dummies.DummyPlayersDF
import com.bbva.datioamproduct.fdevdatio.utils.SuperConfig
import org.apache.spark.sql.DataFrame

class CustomTransformationsTest extends ContextProvider {



  "filterNationalityId method" should "return a DF without values different to 83" in {
    val inputDF = DummyPlayersDF.getDF

    //val inputDF: DataFrame = config.readInputs(PlayersTag)


    val outputDF = inputDF.filterNationalityId(83)

    outputDF.filter(NationalityId.column =!= 83).count() shouldBe 0

  }
}
