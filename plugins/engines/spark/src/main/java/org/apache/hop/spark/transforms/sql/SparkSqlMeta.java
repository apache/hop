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

package org.apache.hop.spark.transforms.sql;

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
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.spark.util.SparkConst;

/**
 * Runs a Spark SQL statement against the Datasets of the incoming transforms, each registered as a
 * temporary view. Native Spark engine only.
 *
 * <p>The output row layout is not inferred from the statement: v1 requires an explicit, non-empty
 * field list so that Hop can resolve fields at design time and so downstream generic mapPartitions
 * transforms see a row layout that matches the Dataset exactly.
 */
@Transform(
    id = SparkConst.SPARK_SQL_PLUGIN_ID,
    name = "i18n::SparkSql.Name",
    description = "i18n::SparkSql.Description",
    image = "spark-sql.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::SparkSql.Keyword",
    documentationUrl = "/pipeline/transforms/spark-sql.html",
    supportedEngines = {SparkConst.PLUGIN_ID})
@Getter
@Setter
public class SparkSqlMeta extends BaseTransformMeta<SparkSql, SparkSqlData> {

  /** SQL executed through {@code SparkSession.sql()}. Variables are resolved driver-side. */
  @HopMetadataProperty(key = "sql", injectionKey = "SQL")
  private String sql;

  /** Optional per-input temporary view name overrides. */
  @HopMetadataProperty(
      groupKey = "views",
      key = "view",
      injectionGroupKey = "VIEWS",
      injectionGroupDescription = "SparkSql.Injection.Group.Views")
  private List<SparkSqlView> views = new ArrayList<>();

  /** Output fields. Required and non-empty in v1 — see the class javadoc. */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "SparkSql.Injection.Group.Fields")
  private List<SparkField> fields = new ArrayList<>();

  public SparkSqlMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return SparkSqlDialog.class.getName();
  }

  /**
   * Derives a SQL-safe temporary view name from a transform name: characters outside {@code
   * [A-Za-z0-9_]} become underscores, and a leading digit is prefixed with an underscore. Transform
   * names such as {@code "Read orders (raw)"} therefore become {@code "Read_orders__raw_"}.
   *
   * <p>Callers that need a stable, readable name should set an explicit override in the {@code
   * views} list instead.
   */
  public static String defaultViewName(String transformName) {
    if (transformName == null || transformName.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder(transformName.length());
    for (int i = 0; i < transformName.length(); i++) {
      char c = transformName.charAt(i);
      sb.append((Character.isLetterOrDigit(c) && c < 128) || c == '_' ? c : '_');
    }
    if (Character.isDigit(sb.charAt(0))) {
      sb.insert(0, '_');
    }
    return sb.toString();
  }

  /** Returns the configured view-name override for {@code transformName}, or null when unset. */
  public String findViewNameOverride(String transformName) {
    if (views == null || transformName == null) {
      return null;
    }
    for (SparkSqlView view : views) {
      if (transformName.equals(view.getTransformName())
          && view.getViewName() != null
          && !view.getViewName().isEmpty()) {
        return view.getViewName();
      }
    }
    return null;
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
    // The statement fully determines the output row: incoming fields do not survive it.
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
      throw new HopTransformException("Unable to create row meta for Spark SQL", e);
    }
  }
}
