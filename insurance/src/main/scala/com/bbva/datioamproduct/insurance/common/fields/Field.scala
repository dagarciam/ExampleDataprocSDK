package com.bbva.datioamproduct.fdevdatio.common.fields

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

//trait o razgo que puede ser heredado para usar sus atributos
trait Field {

  val name: String
  //Una lazy es una val que se evaluara solo cuando se mande a llamar
  lazy val column: Column = col(name)
}
