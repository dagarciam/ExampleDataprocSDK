package com.bbva.datioamproduct.fdevdatio.common

object ConfigConstants {

  val Options: String = "options"
  val IncludeMetadataFields: String = s"$Options.includeMetadataFields"
  val IncludeDeletedFields: String = s"$Options.includeDeletedFields"
  val PartitionOverwriteMode: String = s"$Options.partitionOverwriteMode"
  val CoalesceNumber: String = s"$Options.coalesce"

  val Schema: String = "schema"
  val Path: String = "path"
  val Type: String = "type"
  val SchemaPath: String = s"$Schema.path"
  val OverrideSchema: String = s"$Schema.overrideSchema"
  val MergeSchema: String = s"$Schema.mergeSchema"
  val PartitionOverwriteModeString: String = s"$Options.partitionOverwriteMode"
  val Delimiter: String = s"$Options.delimiter"
  val Header: String = s"$Options.header"
  val Partitions: String = "partitions"
  val Mode: String = "mode"

  val DelimiterOption: String = "delimiter"
  val HeaderOption: String = "header"
  val OverrideSchemaOption: String = "overrideSchema"
  val MergeSchemaOption: String = "mergeSchema"

}
