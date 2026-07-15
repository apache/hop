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

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.percentile_approx;
import static org.apache.spark.sql.functions.stddev_pop;
import static org.apache.spark.sql.functions.sum;

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
import org.apache.hop.pipeline.transforms.memgroupby.GAggregate;
import org.apache.hop.pipeline.transforms.memgroupby.GGroup;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Native Spark implementation of Memory Group By using Dataset groupBy + aggregate (global
 * shuffle).
 */
public class SparkMemoryGroupByHandler extends SparkBaseTransformHandler {

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
          "Memory Group By transform '" + transformMeta.getName() + "' has no input Dataset");
    }

    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    List<String> groupFields = new ArrayList<>();
    for (GGroup group : meta.getGroups()) {
      if (StringUtils.isNotEmpty(group.getField())) {
        groupFields.add(group.getField());
      }
    }

    List<Column> aggCols = new ArrayList<>();
    for (GAggregate aggregate : meta.getAggregates()) {
      aggCols.add(toSparkAgg(aggregate).alias(aggregate.getField()));
    }

    Dataset<Row> output;
    if (groupFields.isEmpty()) {
      // Global aggregation (single group)
      if (aggCols.isEmpty()) {
        throw new HopException(
            "Memory Group By '" + transformMeta.getName() + "' has no group or aggregate fields");
      }
      Column[] aggs = aggCols.toArray(new Column[0]);
      if (aggs.length == 1) {
        output = input.agg(aggs[0]);
      } else {
        output = input.agg(aggs[0], java.util.Arrays.copyOfRange(aggs, 1, aggs.length));
      }
    } else {
      Column[] groupCols = groupFields.stream().map(f -> col(f)).toArray(Column[]::new);
      if (aggCols.isEmpty()) {
        // Distinct groups only
        output = input.select(groupCols).distinct();
      } else {
        Column[] aggs = aggCols.toArray(new Column[0]);
        if (aggs.length == 1) {
          output = input.groupBy(groupCols).agg(aggs[0]);
        } else {
          output =
              input
                  .groupBy(groupCols)
                  .agg(aggs[0], java.util.Arrays.copyOfRange(aggs, 1, aggs.length));
        }
      }
    }

    // Reorder / project to Hop output field order when available
    IRowMeta outputMeta = pipelineMeta.getTransformFields(variables, transformMeta);
    if (outputMeta != null && outputMeta.size() > 0) {
      List<Column> select = new ArrayList<>();
      for (int i = 0; i < outputMeta.size(); i++) {
        String name = outputMeta.getValueMeta(i).getName();
        if (java.util.Arrays.asList(output.columns()).contains(name)) {
          select.add(col(name));
        }
      }
      if (!select.isEmpty()) {
        output = output.select(select.toArray(new Column[0]));
      }
    }

    output = trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM);
    transformDatasetMap.put(transformMeta.getName(), output);
    log.logBasic(
        "Handled Memory Group By (native Spark) : "
            + transformMeta.getName()
            + " groups="
            + groupFields
            + " aggregates="
            + meta.getAggregates().size());
  }

  static Column toSparkAgg(GAggregate aggregate) throws HopException {
    GroupType type = aggregate.getType();
    if (type == null || type == GroupType.None) {
      throw new HopException("Aggregate type is not set for field '" + aggregate.getField() + "'");
    }
    String subject = aggregate.getSubject();
    String valueField = aggregate.getValueField();

    return switch (type) {
      case Sum -> sum(col(subject));
      case Average -> avg(col(subject));
      case Minimum -> min(col(subject));
      case Maximum -> max(col(subject));
      case CountAll -> count(col(subject));
      case CountAny -> count(lit(1));
      case CountDistinct -> countDistinct(col(subject));
      case First -> first(col(subject), true);
      case FirstIncludingNull -> first(col(subject), false);
      case Last -> last(col(subject), true);
      case LastIncludingNull -> last(col(subject), false);
      case StandardDeviation -> stddev_pop(col(subject));
      case Median -> percentile_approx(col(subject), lit(0.5), lit(10000));
      case Percentile -> {
        double p = 0.5;
        if (StringUtils.isNotEmpty(valueField)) {
          try {
            p = Double.parseDouble(valueField) / 100.0;
          } catch (NumberFormatException e) {
            throw new HopException(
                "Invalid percentile value '"
                    + valueField
                    + "' for aggregate "
                    + aggregate.getField(),
                e);
          }
        }
        yield percentile_approx(col(subject), lit(p), lit(10000));
      }
      case ConcatComma -> concat_ws(",", collect_list(col(subject)));
      case ConcatString -> {
        String sep = StringUtils.isNotEmpty(valueField) ? valueField : "";
        yield concat_ws(sep, collect_list(col(subject)));
      }
      case ConcatDistinct -> {
        String sep = StringUtils.isNotEmpty(valueField) ? valueField : ",";
        yield concat_ws(sep, collect_set(col(subject)));
      }
      default ->
          throw new HopException(
              "Aggregate type '"
                  + type.getCode()
                  + "' is not supported by the native Spark Memory Group By handler");
    };
  }
}
