package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.common.example.StaticVals.{FIFTY, NO}
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customers._
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Phones._
import com.bbva.datioamproduct.fdevdatio.common.namings.output.CustomersPhones._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}


object Transformations {

  implicit class PhonesTransformer(df: DataFrame) {

    /**
     * Regla 1
     * Por fecha diaria, tomando únicamente las fechas que comprenden del
     * 2020-03-01 hasta el 2020-03-04 en el campo cutoff_date, recuperando
     * únicamente los celulares que no sean de las marcas “Dell”, “Coolpad”, “Chea”,
     * “BQ” y “BLU", también deberán omitirse los países señalados anteriormente que
     * tienen como country_code “CH”, “IT”, “CZ” y “DK”.
     */
    def filterPhones(): DataFrame = df filter CutoffDate.filter && Brand.filter && CountryCode.filter

  }

  implicit class CustomersTransformer(df: DataFrame) {

    /**
     * Por fecha diaria, tomando únicamente las fechas que comprenden del
     * 2020-03-01 hasta el 2020-03-04 en el campo gl_date, recuperando únicamente
     * a los clientes cuya tarjeta de crédito (credit_card_number) sea menor a 17
     * dígitos.
     */
    def filterCustomers(): DataFrame = {
      df filter GlDate.filter && CreditCardNumber.filter
    }

  }

  implicit class CustomersPhonesTransformer(df: DataFrame) {
    /**
     * Regla 4
     * John Wick quiere identificar los clientes vip mediante un nuevo campo (customer_vip),
     * para que un cliente sea identificado como customer_vip deberá ser miembro prime
     * (prime) y el costo (price_product) del teléfono adquirido deberá ser de 7500.00 en
     * adelante, los clientes vip deben de clasificarse de la siguiente manera:
     * ● “Yes” -> En caso de ser cliente vip.
     * ● “No” -> En caso de no ser cliente vip.
     *
     * Regla 5
     * Los teléfonos tendrán descuentos extra (discount_extra) si son miembros prime, el
     * stock (stock_number) es menor a 35 y sus celulares no son de las siguientes marcas
     * (brand): “XOLO”, “Siemens”,”Panasonic” y ”BlackBerry”.
     * El descuento extra es el 10% del valor original (price_product) del teléfono, en caso de
     * no tener descuento deberá colocarse un 0.00 en dicho campo.
     *
     * Regla 6
     * También se desea saber el precio final (final_price) del teléfono celular, para ello
     * deberás considerar el precio original (price_product), el descuento (discount_amount),
     * el descuento extra (discount_extra) y los impuestos (taxes).
     * El Sr. Wick sabe que a algunos de sus empleados no se le dan bien las matemáticas,
     * así que ha colocado la fórmula del precio final de la siguiente manera:
     * final price = price product + taxes − discount amount − discount extra
     *
     * Regla 7
     * Actualmente no se cuenta con la edad de los clientes, por lo que el Sr. John Wick desea
     * saberlo a partir de su fecha de nacimiento(birth_date), dato que se encuentra en la tabla
     * t_fdev_customers. El Sr. John Wick ha sido muy específico, ha pedido que los años
     * sean calculados a partir del día de hoy, es un jefe muy exigente y necesita conocer las
     * edades cumplidas, no las edades por cumplir (age).
     *
     * Regla 9
     * El Sr. Wick es muy vanidoso y egocéntrico, por lo que desea que la tabla sea
     * particionada por el campo John Wick date (jwk_date). Dicho campo tendrá que ser
     * creado e ir parametrizado dentro del archivo de configuración, ya que el Sr. Wick ama
     * cambiar la fecha de los reportes por pura maldad.
     */
    def addColumn(column: Column): DataFrame = {
      df.select(df.columns.map(col) :+ column: _*)
    }

    /**
     * Regla 8
     * Además el Sr. John Wick ha dado la orden directa de que la promoción sólo aplicará
     * para los teléfonos que estén dentro del top 50 de precios (los más caros) finales
     * (final_price) de cada marca (brand).
     */
    def filterBrandsTop(): DataFrame = {
      addColumn(BrandsTop())
        .filter(BrandsTop.column <= FIFTY)
    }

    /**
     * Regla 10
     * Al Sr. Wick los valores null le recuerdan a su perrito, por lo que ha pedido a sus
     * desarrolladores encargados del reporte, que los registros que sean nulos en el campo
     * nfc (nfc), sean sustituidos por un "No".
     */
    def cleanNfcColumn(): DataFrame = {
      df.na.fill(NO, Seq(Nfc.name))
    }

    /**
     *
     * @return a DataFrame with all columns in the Schema
     */
    def fitToSchema(): DataFrame = {
      df.select(
        CityName.column,
        StreetName.column,
        CreditCardNumber.column,
        LastName.column,
        FirstName.column,
        Age.column,
        Brand.column,
        Model.column,
        Nfc.column,
        CountryCode.column,
        Prime.column,
        CustomerVip.column,
        Taxes(), //Llamamos al apply porque lleva un cast para hacer match con el schema de salida
        PriceProduct.column,
        DiscountAmount(), //Llamamos al apply porque lleva un cast para hacer match con el schema de salida
        ExtraDiscount.column,
        FinalPrice.column,
        BrandsTop.column,
        JwkDate.column
      )
    }

  }

}

