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

package org.apache.hop.spark.pipeline.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.HopSparkRowConverter;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.HopPipelineMetaToSparkConverter;
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.spark.transforms.sql.SparkSqlMeta;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;

/**
 * Native Spark SQL execution. Each incoming transform's Dataset is registered as a temporary view
 * so a single statement can join, union and aggregate them under one Catalyst plan.
 *
 * <p>The statement result is projected onto the transform's declared field list so the Dataset
 * schema matches the Hop row layout that {@link SparkSqlMeta#getFields} reports at design time.
 */
public class SparkSqlHandler extends SparkBaseTransformHandler {

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      String metastoreJson,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, Dataset<Row>> transformDatasetMap,
      SparkSession spark,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      Dataset<Row> input)
      throws HopException {

    SparkSqlMeta meta = new SparkSqlMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String transformName = transformMeta.getName();
    String sql = variables.resolve(meta.getSql());
    if (StringUtils.isBlank(sql)) {
      throw new HopException("Spark SQL '" + transformName + "': the SQL statement is empty");
    }

    List<SparkField> fields = meta.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new HopException(
          "Spark SQL '"
              + transformName
              + "': an explicit output field list is required so Hop can resolve the row layout"
              + " at design time. Add the fields the statement returns.");
    }

    Map<String, String> registeredViews =
        registerInputViews(
            log, variables, meta, transformMeta, previousTransforms, transformDatasetMap);

    Dataset<Row> result;
    try {
      result = spark.sql(sql);
    } catch (Exception e) {
      throw new HopException(
          "Spark SQL '"
              + transformName
              + "': the statement could not be executed. Registered views: "
              + registeredViews.values()
              + ". SQL: "
              + sql,
          e);
    }

    Dataset<Row> output = projectDeclaredFields(result, fields, transformName);
    output = trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM);
    transformDatasetMap.put(transformName, output);

    log.logBasic(
        "Handled Spark SQL : "
            + transformName
            + " views="
            + registeredViews
            + " columns="
            + Arrays.toString(output.columns()));
  }

  /**
   * Registers one temporary view per incoming transform and returns the transform-name to view-name
   * mapping. An empty map means a zero-input statement (catalog tables, {@code VALUES}, …).
   */
  private Map<String, String> registerInputViews(
      ILogChannel log,
      IVariables variables,
      SparkSqlMeta meta,
      TransformMeta transformMeta,
      List<TransformMeta> previousTransforms,
      Map<String, Dataset<Row>> transformDatasetMap)
      throws HopException {

    Map<String, String> registered = new LinkedHashMap<>();
    if (previousTransforms == null || previousTransforms.isEmpty()) {
      return registered;
    }

    // view name -> source transform, to report collisions rather than silently shadowing
    Map<String, String> viewOwners = new LinkedHashMap<>();

    for (TransformMeta previous : previousTransforms) {
      String previousName = previous.getName();
      Dataset<Row> dataset =
          HopPipelineMetaToSparkConverter.lookupPreviousDataset(
              transformDatasetMap, previous, transformMeta, log);
      if (dataset == null) {
        throw new HopException(
            "Spark SQL '"
                + transformMeta.getName()
                + "': the Dataset for incoming transform '"
                + previousName
                + "' could not be found. Check that the hop into this transform is enabled.");
      }

      String override = meta.findViewNameOverride(previousName);
      String viewName =
          StringUtils.isNotEmpty(override)
              ? variables.resolve(override)
              : SparkSqlMeta.defaultViewName(previousName);
      if (StringUtils.isBlank(viewName)) {
        throw new HopException(
            "Spark SQL '"
                + transformMeta.getName()
                + "': could not determine a view name for incoming transform '"
                + previousName
                + "'");
      }

      String owner = viewOwners.put(viewName, previousName);
      if (owner != null) {
        throw new HopException(
            "Spark SQL '"
                + transformMeta.getName()
                + "': incoming transforms '"
                + owner
                + "' and '"
                + previousName
                + "' both map to view '"
                + viewName
                + "'. Set distinct view names in the Input views tab.");
      }

      dataset.createOrReplaceTempView(viewName);
      registered.put(previousName, viewName);
    }
    return registered;
  }

  /**
   * Selects the declared fields, in declared order, casting each to the Spark type for its Hop
   * type. Guarantees the Dataset schema matches the row layout reported at design time.
   */
  private Dataset<Row> projectDeclaredFields(
      Dataset<Row> result, List<SparkField> fields, String transformName) throws HopException {

    String[] resultColumns = result.columns();
    List<Column> projection = new ArrayList<>(fields.size());

    for (SparkField field : fields) {
      String name = field.getName();
      if (StringUtils.isBlank(name)) {
        throw new HopException(
            "Spark SQL '" + transformName + "': a declared output field has no name");
      }

      String actual = findColumn(resultColumns, name);
      if (actual == null) {
        throw new HopException(
            "Spark SQL '"
                + transformName
                + "': declared output field '"
                + name
                + "' is not returned by the statement. Columns returned: "
                + Arrays.toString(resultColumns));
      }

      IValueMeta valueMeta;
      try {
        valueMeta = field.createValueMeta();
      } catch (Exception e) {
        throw new HopException(
            "Spark SQL '"
                + transformName
                + "': unable to build the value metadata for output field '"
                + name
                + "'",
            e);
      }
      DataType dataType = HopSparkRowConverter.toDataType(valueMeta);
      projection.add(result.col(actual).cast(dataType).alias(name));
    }

    return result.select(projection.toArray(new Column[0]));
  }

  /**
   * Resolves a declared field against the statement's columns, preferring an exact match and
   * falling back to a case-insensitive one (Spark resolves case-insensitively by default).
   */
  private static String findColumn(String[] resultColumns, String name) {
    for (String column : resultColumns) {
      if (column.equals(name)) {
        return column;
      }
    }
    for (String column : resultColumns) {
      if (column.equalsIgnoreCase(name)) {
        return column;
      }
    }
    return null;
  }
}
