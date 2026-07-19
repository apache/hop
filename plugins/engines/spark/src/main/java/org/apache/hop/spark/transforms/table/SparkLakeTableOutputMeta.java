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

package org.apache.hop.spark.transforms.table;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.table.SparkLakeFormats;
import org.apache.hop.spark.transforms.io.SparkFileOutputMeta;
import org.apache.hop.spark.util.SparkConst;

@Transform(
    id = SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID,
    name = "i18n::SparkLakeTableOutput.Name",
    description = "i18n::SparkLakeTableOutput.Description",
    image = "spark-lake-table-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::SparkLakeTableOutput.Keyword",
    documentationUrl = "/pipeline/transforms/spark-lake-table-output.html",
    supportedEngines = {SparkConst.PLUGIN_ID})
@Getter
@Setter
public class SparkLakeTableOutputMeta
    extends BaseTransformMeta<SparkLakeTableOutput, SparkLakeTableOutputData> {

  /** {@link SparkLakeFormats#FORMAT_DELTA} or {@link SparkLakeFormats#FORMAT_ICEBERG} */
  @HopMetadataProperty(key = "format", injectionKey = "FORMAT")
  private String format = SparkLakeFormats.FORMAT_DELTA;

  @HopMetadataProperty(key = "identifier_mode", injectionKey = "IDENTIFIER_MODE")
  private String identifierMode = SparkLakeTableInputMeta.MODE_PATH;

  @HopMetadataProperty(key = "table_path", injectionKey = "TABLE_PATH")
  private String tablePath;

  @HopMetadataProperty(key = "table_identifier", injectionKey = "TABLE_IDENTIFIER")
  private String tableIdentifier;

  @HopMetadataProperty(key = "catalog_metadata_name", injectionKey = "CATALOG_METADATA_NAME")
  private String catalogMetadataName;

  /**
   * Default {@link SparkFileOutputMeta#MODE_ERROR} (ErrorIfExists) — ACID tables must not default
   * to destructive overwrite.
   */
  @HopMetadataProperty(key = "save_mode", injectionKey = "SAVE_MODE")
  private String saveMode = SparkFileOutputMeta.MODE_ERROR;

  @HopMetadataProperty(key = "partition_by", injectionKey = "PARTITION_BY")
  private String partitionByColumns;

  @HopMetadataProperty(key = "coalesce", injectionKey = "COALESCE")
  private String coalescePartitions;

  /** Extra Spark write options as key=value lines */
  @HopMetadataProperty(key = "extra_options", injectionKey = "EXTRA_OPTIONS")
  private String extraOptions;

  public SparkLakeTableOutputMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return SparkLakeTableOutputDialog.class.getName();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Pass-through
  }
}
