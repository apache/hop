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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.table.SparkLakeFormats;
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.spark.util.SparkConst;

@Transform(
    id = SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID,
    name = "i18n::SparkLakeTableInput.Name",
    description = "i18n::SparkLakeTableInput.Description",
    image = "spark-lake-table-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::SparkLakeTableInput.Keyword",
    documentationUrl = "/pipeline/transforms/spark-lake-table-input.html",
    supportedEngines = {SparkConst.PLUGIN_ID})
@Getter
@Setter
public class SparkLakeTableInputMeta
    extends BaseTransformMeta<SparkLakeTableInput, SparkLakeTableInputData> {

  public static final String MODE_PATH = "PATH";
  public static final String MODE_TABLE = "TABLE";

  public static final String TIME_TRAVEL_NONE = "NONE";
  public static final String TIME_TRAVEL_VERSION = "VERSION";
  public static final String TIME_TRAVEL_TIMESTAMP = "TIMESTAMP";

  /** {@link SparkLakeFormats#FORMAT_DELTA} or {@link SparkLakeFormats#FORMAT_ICEBERG} */
  @HopMetadataProperty(key = "format")
  private String format = SparkLakeFormats.FORMAT_DELTA;

  /** PATH (v1 primary) or TABLE (catalog — later PRs) */
  @HopMetadataProperty(key = "identifier_mode")
  private String identifierMode = MODE_PATH;

  @HopMetadataProperty(key = "table_path")
  private String tablePath;

  /** Catalog-qualified table id when mode is TABLE (e.g. lake.db.orders). */
  @HopMetadataProperty(key = "table_identifier")
  private String tableIdentifier;

  /** Hop SparkCatalog metadata name when mode is TABLE. */
  @HopMetadataProperty(key = "catalog_metadata_name")
  private String catalogMetadataName;

  /**
   * Time travel: {@link #TIME_TRAVEL_NONE}, {@link #TIME_TRAVEL_VERSION}, or {@link
   * #TIME_TRAVEL_TIMESTAMP}.
   */
  @HopMetadataProperty(key = "time_travel_type")
  private String timeTravelType = TIME_TRAVEL_NONE;

  /** Delta version number or Iceberg snapshot id (when type is VERSION). Supports variables. */
  @HopMetadataProperty(key = "time_travel_version")
  private String timeTravelVersion;

  /**
   * Spark-parseable timestamp string (when type is TIMESTAMP), e.g. {@code 2024-01-15 10:30:00}.
   */
  @HopMetadataProperty(key = "time_travel_timestamp")
  private String timeTravelTimestamp;

  /** Extra Spark read options as key=value lines */
  @HopMetadataProperty(key = "extra_options")
  private String extraOptions;

  /** Optional projection (name-based select/cast). */
  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<SparkField> fields = new ArrayList<>();

  public SparkLakeTableInputMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return SparkLakeTableInputDialog.class.getName();
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
    inputRowMeta.clear();
    if (fields == null || fields.isEmpty()) {
      return;
    }
    try {
      for (SparkField field : fields) {
        if (field.getName() != null && !field.getName().isEmpty()) {
          inputRowMeta.addValueMeta(field.createValueMeta());
        }
      }
    } catch (HopPluginException e) {
      throw new HopTransformException("Unable to create row meta for Spark Lake Table Input", e);
    }
  }
}
