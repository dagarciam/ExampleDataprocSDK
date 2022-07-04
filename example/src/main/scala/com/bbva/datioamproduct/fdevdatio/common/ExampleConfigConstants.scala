package com.bbva.datioamproduct.fdevdatio.common

object ExampleConfigConstants {

  val RootConfig: String = "exampleJob"
  val InputConfig: String = s"$RootConfig.input"
  val OutputConfig: String = s"$RootConfig.output"
  val ParamsConfig: String = s"$RootConfig.params"

  val CustomersConfig: String = s"$InputConfig.t_fdev_customers"
  val PhonesConfig: String = s"$InputConfig.t_fdev_phones"
  val CustomersPhonesConfig: String = s"$OutputConfig.t_fdev_customersphones"

  val JwkDate: String = s"$ParamsConfig.jwk_date"

}
