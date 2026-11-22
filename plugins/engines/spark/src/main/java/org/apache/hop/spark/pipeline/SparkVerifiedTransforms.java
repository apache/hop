/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.pipeline;

import java.util.Set;

/**
 * Transforms without a native Spark handler that give the same rows on the native Spark engine as
 * on the Hop engine, as partition-local Hop mini-pipelines. {@link
 * org.apache.hop.spark.engines.SparkPipelineEngine#supports} answers "supported" for them, and
 * their documentation page says "Native Spark=yes". A transform that is neither here, nor refused
 * ({@link HopPipelineMetaToSparkConverter#HARD_BANNED_PLUGIN_IDS}), nor handled natively ({@link
 * HopPipelineMetaToSparkConverter#EXPLICIT_HANDLER_PLUGIN_IDS}) may work, but no test shows it.
 *
 * <p>Add sequence, Sample rows and Reservoir sampling are not listed: the integration test baseline
 * ran them on one partition only, and on several partitions they give other rows (Add sequence
 * repeats its values per partition, Sample rows samples each partition).
 */
public final class SparkVerifiedTransforms {

  /** Plugin ids of the verified transforms. */
  public static final Set<String> PLUGIN_IDS =
      Set.of(
          "Abort",
          "AccessOutput",
          "AddXML",
          "AdvancedXMLOutput",
          "AvroDecode",
          "AvroEncode",
          "AvroFileInput",
          "AvroOutput",
          "BinaryFileOutput",
          "CSVInput",
          "Calculator",
          "ChangeFileEncoding",
          "CheckSum",
          "CloneRow",
          "Coalesce",
          "ColumnExists",
          "CombinationLookup",
          "ConcatFields",
          "Constant",
          "CreditCardValidator",
          "CubeInput",
          "CubeOutput",
          "DBJoin",
          "DBLookup",
          "DDL",
          "DataGrid",
          "DataSetInput",
          "DataSetOutput",
          "DataStreamInput",
          "DataStreamOutput",
          "DatabaseValueValidation",
          "DbImpactInput",
          "Delay",
          "Delete",
          "Denormaliser",
          "DetectLanguage",
          "DimensionLookup",
          "Dummy",
          "DynamicSqlRow",
          "EnhancedJsonOutput",
          "ExcelInput",
          "ExecProcess",
          "ExecSql",
          "ExecSqlRow",
          "ExecuteTests",
          "Fake",
          "FieldSplitter",
          "FileExists",
          "FileLocked",
          "FileMetadataPlugin",
          "FilterRows",
          "Flattener",
          "Formula",
          "FuzzyMatch",
          "GetFileNames",
          "GetFilesRowsCount",
          "GetGraphSchema",
          "GetSubFolders",
          "GetVariable",
          "GitInput",
          "Html2Text",
          "Http",
          "HttpPost",
          "IfNull",
          "InsertUpdate",
          "Janino",
          "JavaFilter",
          "JdbcMetadata",
          "JsonInput",
          "JsonNormalizeInput",
          "JsonOutput",
          "LoadFileInput",
          "MailInput",
          "MappingInput",
          "MappingOutput",
          "MetadataInput",
          "MonetDBBulkLoader",
          "MongoDbDelete",
          "MongoDbInput",
          "MongoDbOutput",
          "MsSqlServerBulkLoader",
          "MySqlBulkLoader",
          "Neo4JOutput",
          "Neo4jCypherBuilder",
          "Neo4jCypherOutput",
          "Neo4jGraphOutput",
          "Normaliser",
          "NullIf",
          "NumberRange",
          "ODataInput",
          "OpenFormula",
          "PGPDecryptStream",
          "PGPEncryptStream",
          "ParquetFileInput",
          "ParquetFileOutput",
          "PgVectorSearch",
          "PgVectorUpsert",
          "PipelineExecutor",
          "PluginCatalog",
          "ProcessFiles",
          "PropertyInput",
          "PropertyOutput",
          "RandomValue",
          "RedisInput",
          "RedisOutput",
          "RegexEval",
          "ReplaceString",
          "Rest",
          "RowGenerator",
          "RuleExecutor",
          "SFTPPut",
          "SQLFileOutput",
          "SSH",
          "SchemaMapping",
          "ScriptValueMod",
          "SelectValues",
          "SetValueConstant",
          "SetValueField",
          "SimpleMapping",
          "SnowflakeId",
          "SplitFieldToRows3",
          "StandardizePhoneNumber",
          "StreamLookup",
          "StringCut",
          "StringOperations",
          "SuperScript",
          "SurefireReportOutput",
          "SwitchCase",
          "SynchronizeAfterMerge",
          "SystemInfo",
          "TableCompare",
          "TableExists",
          "TableOutput",
          "TextChunker",
          "TextFileInput2",
          "TextFileOutput",
          "Tika",
          "TokenReplacementPlugin",
          "TransformMetaStructure",
          "TransformsMetrics",
          "TypeExitEdi2XmlTransform",
          "TypeExitExcelWriterTransform",
          "Update",
          "UserDefinedJavaClass",
          "VCardInput",
          "VCardOutput",
          "Validator",
          "ValueMapper",
          "VerticaBulkLoader",
          "WatchFiles",
          "WebServiceAvailable",
          "WorkflowExecutor",
          "WriteToLog",
          "XMLInputStream",
          "XMLOutput",
          "XSDValidator",
          "XSLT",
          "YamlInput",
          "ZipFile",
          "getXMLData");

  private SparkVerifiedTransforms() {
    // Constants only
  }
}
