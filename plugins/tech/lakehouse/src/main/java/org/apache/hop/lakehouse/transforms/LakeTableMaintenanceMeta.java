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
    id = LakehouseConst.LAKE_TABLE_MAINTENANCE_PLUGIN_ID,
    name = "i18n::SparkLakeTableMaintenance.Name",
    description = "i18n::SparkLakeTableMaintenance.Description",
    image = "spark-lake-table-maintenance.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::SparkLakeTableMaintenance.Keyword",
    documentationUrl = "/pipeline/transforms/spark-lake-table-maintenance.html",
    supportedEngines = {LakehouseConst.SPARK_ENGINE_ID})
@Getter
@Setter
public class LakeTableMaintenanceMeta
    extends BaseTransformMeta<LakeTableMaintenance, LakeTableMaintenanceData> {

  public static final String OP_OPTIMIZE = "OPTIMIZE";
  public static final String OP_VACUUM = "VACUUM";
  public static final String OP_EXPIRE_SNAPSHOTS = "EXPIRE_SNAPSHOTS";
  public static final String OP_REWRITE_MANIFESTS = "REWRITE_MANIFESTS";
  public static final String OP_DELETE_WHERE = "DELETE_WHERE";

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

  /** The OP_* operation constants above. */
  @HopMetadataProperty(key = "operation", injectionKey = "OPERATION")
  private String operation = LakeTableMaintenanceMeta.OP_OPTIMIZE;

  /** Required for VACUUM / EXPIRE_SNAPSHOTS (hours). No silent default. */
  @HopMetadataProperty(key = "retention_hours", injectionKey = "RETENTION_HOURS")
  private String retentionHours;

  /** Iceberg expire_snapshots retain_last (default 1 when retentionHours set). */
  @HopMetadataProperty(key = "retain_last", injectionKey = "RETAIN_LAST")
  private String retainLast;

  /** Optional WHERE for OPTIMIZE / DELETE_WHERE. */
  @HopMetadataProperty(key = "where_clause", injectionKey = "WHERE_CLAUSE")
  private String whereClause;

  /** Optional Delta OPTIMIZE ZORDER BY columns (comma-separated). */
  @HopMetadataProperty(key = "zorder_columns", injectionKey = "ZORDER_COLUMNS")
  private String zOrderColumns;

  /**
   * Must be true for destructive ops (VACUUM, EXPIRE_SNAPSHOTS, DELETE_WHERE). Operator
   * acknowledgement that data files / snapshots may be removed.
   */
  @HopMetadataProperty(key = "acknowledge_destructive", injectionKey = "ACKNOWLEDGE_DESTRUCTIVE")
  private boolean acknowledgeDestructive;

  public LakeTableMaintenanceMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return LakeTableMaintenanceDialog.class.getName();
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
    // Zero-input action sink
  }
}
