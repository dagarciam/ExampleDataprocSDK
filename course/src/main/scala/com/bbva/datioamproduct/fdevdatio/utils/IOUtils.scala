package com.bbva.datioamproduct.fdevdatio.utils

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstantsCourse._
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.io.output.DatioDataFrameWriter
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.net.URI

trait IOUtils {
  lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()

  def read(inputConfig: Config): DataFrame = {
    val path: String = inputConfig.getString(Path)

    inputConfig.getString(Type) match {
      case "parquet" =>
        val schemaPath: String = inputConfig.getString(SchemaPath)
        val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
        val overrideSchema: String = inputConfig.getString(OverrideSchema)
        val mergeSchema: String = inputConfig.getString(MergeSchema)

        datioSparkSession.read()
          .option(OverrideSchemaOption, overrideSchema)
          .option(MergeSchemaOption, mergeSchema)
          .datioSchema(schema)
          .parquet(path)

      case "csv" =>
        val schemaPath: String = inputConfig.getString(SchemaPath)
        val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
        val overrideSchema: String = inputConfig.getString(OverrideSchema)
        val mergeSchema: String = inputConfig.getString(MergeSchema)

        val delimiter: String = inputConfig.getString(Delimiter)
        val header: String = inputConfig.getString(Header)

        datioSparkSession.read()
          .option(OverrideSchemaOption, overrideSchema)
          .option(MergeSchemaOption, mergeSchema)
          .option(DelimiterOption, delimiter)
          .option(HeaderOption, header)
          .datioSchema(schema)
          .csv(path)

      case "avro" =>
        val schemaPath: String = inputConfig.getString(SchemaPath)
        val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
        datioSparkSession.read()
          .datioSchema(schema)
          .avro(path)

      case _@inputType => throw new Exception(s"Formato de archivo no soportado: $inputType")
    }
  }

  def write(df: DataFrame, outputConfig: Config): Unit = {
    val mode: SaveMode = outputConfig.getString(Mode) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case _@saveMode => throw new Exception(s"Modo de escritura no soportado: $saveMode")
    }

    val path: String = outputConfig.getString(Path)

    val schemaPath: String = outputConfig.getString(SchemaPath)
    val icludeMetadata: Boolean = outputConfig.getBoolean(IncludeMetadataFields)
    val icludeDeletedFields: Boolean = outputConfig.getBoolean(IncludeDeletedFields)

    val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath))
      .withMetadataFields(icludeMetadata)
      .withDeletedFields(icludeDeletedFields)
      .build()

    val partitions: Array[String] = outputConfig.getStringList(Partitions).toArray.map(_.toString)
    val partitionOverwriteMode: String = outputConfig.getString(PartitionOverwriteMode)

    val numRepartition: Int = outputConfig.getInt(Repartition)
    val finalDF: DataFrame = df.repartition(numRepartition)

    val writer: DatioDataFrameWriter = datioSparkSession
      .write()
      .mode(mode)
      .option(PartitionOverwriteModeString, partitionOverwriteMode)
      .datioSchema(schema)
      .partitionBy(partitions: _*)

    outputConfig.getString(Type) match {
      case "parquet" => writer.parquet(finalDF, path)
      case "csv" => writer.csv(finalDF, path)
      case "avro" => writer.avro(finalDF, path)
      case _@outputType => throw new Exception(s"Formato de escritura no soportado: $outputType")
    }

  }

}


