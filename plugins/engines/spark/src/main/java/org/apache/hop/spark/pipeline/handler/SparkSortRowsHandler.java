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

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.sort.SortRowsField;
import org.apache.hop.pipeline.transforms.sort.SortRowsMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Native Spark global sort (orderBy) for Sort Rows. */
public class SparkSortRowsHandler extends SparkBaseTransformHandler {

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

    if (input == null) {
      throw new HopException(
          "Sort Rows transform '" + transformMeta.getName() + "' has no input Dataset");
    }

    SortRowsMeta meta = new SortRowsMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    List<SortRowsField> sortFields = meta.getSortFields();
    if (sortFields == null || sortFields.isEmpty()) {
      throw new HopException(
          "Sort Rows '" + transformMeta.getName() + "' has no sort fields configured");
    }

    List<Column> orderCols = new ArrayList<>();
    for (SortRowsField field : sortFields) {
      if (StringUtils.isEmpty(field.getFieldName())) {
        continue;
      }
      Column c = col(field.getFieldName());
      // Hop: ascending=true means ASC; false means DESC
      orderCols.add(field.isAscending() ? c.asc() : c.desc());
    }
    if (orderCols.isEmpty()) {
      throw new HopException(
          "Sort Rows '" + transformMeta.getName() + "' has no valid sort field names");
    }

    Dataset<Row> output = input.orderBy(orderCols.toArray(new Column[0]));

    if (meta.isOnlyPassingUniqueRows()) {
      // Unique on the sort keys (Hop behavior approximates sorted unique)
      String[] keys =
          sortFields.stream()
              .map(SortRowsField::getFieldName)
              .filter(StringUtils::isNotEmpty)
              .toArray(String[]::new);
      if (keys.length > 0) {
        output = output.dropDuplicates(keys);
      } else {
        output = output.dropDuplicates();
      }
    }

    output = trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM);
    transformDatasetMap.put(transformMeta.getName(), output);
    log.logBasic(
        "Handled Sort Rows (native Spark) : "
            + transformMeta.getName()
            + " fields="
            + sortFields.size()
            + " unique="
            + meta.isOnlyPassingUniqueRows());
  }
}
