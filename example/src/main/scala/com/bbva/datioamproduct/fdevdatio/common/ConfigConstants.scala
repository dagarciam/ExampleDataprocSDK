package com.bbva.datioamproduct.fdevdatio.common

object ConfigConstants {

  val Options: String = "options"
  val IncludeMetadataFields: String = s"$Options.includeMetadataFields"
  val IncludeDeletedFields: String = s"$Options.includeDeletedFields"
  val PartitionOverwriteMode: String = s"$Options.partitionOverwriteMode"

  val Path: String = "path"
  val Type: String = "type"
  val SchemaPath: String = "schema.path"
  val PartitionOverwriteModeString: String = "partitionOverwriteMode"
  val Delimiter: String = "delimiter"
  val Header: String = "header"
  val Partitions: String = "partitions"
  val Mode: String = "mode"

}
