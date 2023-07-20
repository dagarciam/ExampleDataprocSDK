package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants.Table
import com.bbva.datioamproduct.fdevdatio.common.ExampleConfigConstants.{CustomersConfig, PhonesConfig}
import com.bbva.datioamproduct.fdevdatio.common.example.StaticVals.JoinTypes
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customers.{CustomerId, DeliveryId, _}
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Phones._
import com.bbva.datioamproduct.fdevdatio.common.namings.output.CustomersPhones._
import com.bbva.datioamproduct.fdevdatio.utils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations.{CustomersPhonesTransformer, CustomersTransformer, PhonesTransformer}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

class TransformationsTest extends ContextProvider {

  "filterPhones method" should
    "return a DF without values CH, IT, CZ y DK in column country_code" in {
    val inputDF: PhonesTransformer = datioSparkSession.read
      .table(config.getString(s"$PhonesConfig.$Table"))
    val outputDF = inputDF.filterPhones()

    outputDF
      .filter(CountryCode.column.isin("CH", "IT", "CZ", "DK")).count() shouldBe 0
  }

  "filterCustomers method" should
    "return a DF with shortest lengths than 17 in column credit_card_number" in {

    val inputDF: CustomersTransformer = datioSparkSession.read
      .table(config.getString(s"$CustomersConfig.$Table"))
    val outputDF = inputDF.filterCustomers()

    outputDF
      .filter(!CreditCardNumber.filter).count() shouldBe 0
  }

  "addColumn method" should
    "add a Column to the DataFrame" in {

    val phonesDF: PhonesTransformer = datioSparkSession.read
      .table(config.getString(s"$PhonesConfig.$Table"))
    val customersDF: CustomersTransformer = datioSparkSession.read
      .table(config.getString(s"$CustomersConfig.$Table"))

    val inputDF = phonesDF.filterPhones().join(
      customersDF.filterCustomers(),
      Seq(CustomerId.name, DeliveryId.name),
      JoinTypes.INNER
    )

    val testColumn: Column = lit("example_value").alias("test_column")
    val outputDF = inputDF.addColumn(testColumn)

    outputDF.columns should contain("test_column")
  }

  "filterBrandsTop method" should
    "add one Column brands_top and return a DF with values less than 51" in {

    val phonesDF: PhonesTransformer = datioSparkSession.read
      .table(config.getString(s"$PhonesConfig.$Table"))
    val customersDF: CustomersTransformer = datioSparkSession.read
      .table(config.getString(s"$CustomersConfig.$Table"))

    val inputDF = phonesDF.filterPhones().join(
      customersDF.filterCustomers(),
      Seq(CustomerId.name, DeliveryId.name),
      JoinTypes.INNER
    ).addColumn(CustomerVip())
      .addColumn(ExtraDiscount())
      .addColumn(FinalPrice()) // Necesitamos final_price para probar la regla que genera brands_top

    val outputDF = inputDF.filterBrandsTop()

    inputDF.columns should not contain "brands_top"
    outputDF.columns should contain("brands_top")
    outputDF.filter(BrandsTop.column > 50).count() should be(0)

  }
}
