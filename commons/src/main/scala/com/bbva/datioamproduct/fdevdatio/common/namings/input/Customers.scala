package com.bbva.datioamproduct.fdevdatio.common.namings.input

import com.bbva.datioamproduct.fdevdatio.common.namings.Field
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.length

object Customers {

  case object CustomerId extends Field {
    override val name: String = "customer_id"
  }

  case object Zipcode extends Field {
    override val name: String = "zipcode"
  }

  case object CityName extends Field {
    override val name: String = "city_name"
  }

  case object StreetName extends Field {
    override val name: String = "street_name"
  }

  case object CreditCardNumber extends Field {
    override val name: String = "credit_card_number"
    val filter: Column = length(column) < 17
  }

  case object CreditProviderName extends Field {
    override val name: String = "credit_provider_name"
  }

  case object CompanyName extends Field {
    override val name: String = "company_name"
  }

  case object CompanyEmail extends Field {
    override val name: String = "company_email"
  }

  case object LastName extends Field {
    override val name: String = "last_name"
  }

  case object FirstName extends Field {
    override val name: String = "first_name"
  }

  case object DeliveryId extends Field {
    override val name: String = "delivery_id"
  }

  case object BirthDate extends Field {
    override val name: String = "birth_date"
  }

  case object GlDate extends Field {
    override val name: String = "gl_date"
    val filter: Column = column.between("2020-03-01", "2020-03-04")
  }

}
