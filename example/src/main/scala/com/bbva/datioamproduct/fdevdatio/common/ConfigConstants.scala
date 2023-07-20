package com.bbva.datioamproduct.fdevdatio.common

object ConfigConstants {

  val Options: String = "options"
  val OverrideSchema: String = s"$Options.overrideSchema"
  val MergeSchema: String = s"$Options.mergeSchema"
  val PartitionOverwriteMode: String = s"$Options.partitionOverwriteMode"
  val CoalesceNumber: String = s"$Options.coalesce"
  val Delimiter: String = s"$Options.delimiter"
  val Header: String = s"$Options.header"

  val Schema: String = "schema"
  val SchemaPath: String = s"$Schema.path"
  val IncludeMetadataFields: String = s"$Schema.includeMetadataFields"
  val IncludeDeletedFields: String = s"$Schema.includeDeletedFields"


  val Path: String = "path"
  val Table: String = "table"
  val Type: String = "type"

  val PartitionOverwriteModeString: String = s"$Options.partitionOverwriteMode"
  val Partitions: String = "partitions"
  val Mode: String = "mode"

  val DelimiterOption: String = "delimiter"
  val HeaderOption: String = "header"
  val OverrideSchemaOption: String = "overrideSchema"
  val MergeSchemaOption: String = "mergeSchema"

}
