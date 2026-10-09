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

package org.apache.hop.lakehouse.transforms;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lakehouse.LakeFormats;
import org.apache.hop.lakehouse.LakehouseConst;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = LakehouseConst.LAKE_TABLE_MERGE_PLUGIN_ID,
    name = "i18n::SparkLakeTableMerge.Name",
    description = "i18n::SparkLakeTableMerge.Description",
    image = "spark-lake-table-merge.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::SparkLakeTableMerge.Keyword",
    documentationUrl = "/pipeline/transforms/spark-lake-table-merge.html",
    supportedEngines = {LakehouseConst.SPARK_ENGINE_ID})
@Getter
@Setter
public class LakeTableMergeMeta extends BaseTransformMeta<LakeTableMerge, LakeTableMergeData> {

  public static final String MATCHED_UPDATE_ALL = "UPDATE_ALL";
  public static final String MATCHED_DELETE = "DELETE";
  public static final String MATCHED_NONE = "NONE";

  public static final String NOT_MATCHED_INSERT_ALL = "INSERT_ALL";
  public static final String NOT_MATCHED_NONE = "NONE";

  public static final String NOT_MATCHED_BY_SOURCE_DELETE = "DELETE";
  public static final String NOT_MATCHED_BY_SOURCE_NONE = "NONE";

  @HopMetadataProperty(key = "format", injectionKey = "FORMAT")
  private String format = LakeFormats.FORMAT_DELTA;

  @HopMetadataProperty(key = "identifier_mode", injectionKey = "IDENTIFIER_MODE")
  private String identifierMode = LakeTableInputMeta.MODE_PATH;

  @HopMetadataProperty(key = "table_path", injectionKey = "TABLE_PATH")
  private String tablePath;

  @HopMetadataProperty(key = "table_identifier", injectionKey = "TABLE_IDENTIFIER")
  private String tableIdentifier;

  @HopMetadataProperty(key = "catalog_metadata_name", injectionKey = "CATALOG_METADATA_NAME")
  private String catalogMetadataName;

  /** ON clause, e.g. {@code t.id = s.id} (t = target, s = source). */
  @HopMetadataProperty(key = "merge_condition", injectionKey = "MERGE_CONDITION")
  private String mergeCondition;

  /** {@link #MATCHED_UPDATE_ALL}, DELETE, or NONE */
  @HopMetadataProperty(key = "matched_action", injectionKey = "MATCHED_ACTION")
  private String matchedAction = LakeTableMergeMeta.MATCHED_UPDATE_ALL;

  /** {@link #NOT_MATCHED_INSERT_ALL} or NONE */
  @HopMetadataProperty(key = "not_matched_action", injectionKey = "NOT_MATCHED_ACTION")
  private String notMatchedAction = LakeTableMergeMeta.NOT_MATCHED_INSERT_ALL;

  /**
   * Optional {@link #NOT_MATCHED_BY_SOURCE_DELETE} (Delta; Iceberg support varies). Default NONE.
   */
  @HopMetadataProperty(
      key = "not_matched_by_source_action",
      injectionKey = "NOT_MATCHED_BY_SOURCE_ACTION")
  private String notMatchedBySourceAction = LakeTableMergeMeta.NOT_MATCHED_BY_SOURCE_NONE;

  /**
   * Advanced: full MERGE SQL. When non-empty, overrides structured fields. Operator is trusted;
   * empty by default.
   */
  @HopMetadataProperty(key = "raw_merge_sql", injectionKey = "RAW_MERGE_SQL")
  private String rawMergeSql;

  public LakeTableMergeMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return LakeTableMergeDialog.class.getName();
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
    // Action sink — no outgoing fields on native Spark
  }
}
