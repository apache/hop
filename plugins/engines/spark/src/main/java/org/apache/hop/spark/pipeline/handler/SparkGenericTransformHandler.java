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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.HopMapPartitionsFn;
import org.apache.hop.spark.core.HopSparkRowConverter;
import org.apache.hop.spark.core.SparkExecutionDataAccumulator;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.core.SparkVariableValue;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.ISparkPipelineTransformHandler;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Wraps a Hop transform in {@link HopMapPartitionsFn} so initialization happens once per Spark
 * partition.
 */
public class SparkGenericTransformHandler implements ISparkPipelineTransformHandler {

  private SparkTransformMetricsAccumulator metricsAccumulator;
  private SparkExecutionDataAccumulator sampleDataAccumulator;
  private String runConfigName;
  private String parentLogChannelId;
  private String dataSamplersJson;

  public void setMetricsAccumulator(SparkTransformMetricsAccumulator metricsAccumulator) {
    this.metricsAccumulator = metricsAccumulator;
  }

  public void setSampleDataAccumulator(SparkExecutionDataAccumulator sampleDataAccumulator) {
    this.sampleDataAccumulator = sampleDataAccumulator;
  }

  public void setExecutionSamplingContext(
      String runConfigName, String parentLogChannelId, String dataSamplersJson) {
    this.runConfigName = runConfigName;
    this.parentLogChannelId = parentLogChannelId;
    this.dataSamplersJson = dataSamplersJson;
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

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

    boolean inputTransform = input == null;
    if (previousTransforms != null && previousTransforms.size() > 1) {
      throw new HopException(
          "Combining data from multiple previous transforms is not yet supported for transform '"
              + transformMeta.getName()
              + "' on the native Spark engine (single main input only in v1)");
    }

    List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transformMeta);
    if (nextTransforms.size() > 1) {
      throw new HopException(
          "Multiple target hops from transform '"
              + transformMeta.getName()
              + "' are not yet supported on the native Spark engine");
    }

    IRowMeta outputRowMeta = pipelineMeta.getTransformFields(variables, transformMeta);
    StructType outputSchema = HopSparkRowConverter.toStructType(outputRowMeta);

    String transformMetaXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + transformMeta.getTransform().getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    List<SparkVariableValue> variableValues = collectVariables(variables);
    String inputRowMetaJson =
        rowMeta != null && rowMeta.size() > 0 ? JsonRowMeta.toJson(rowMeta) : null;
    String outputRowMetaJson = JsonRowMeta.toJson(outputRowMeta);

    HopMapPartitionsFn mapFn =
        new HopMapPartitionsFn(
            variableValues,
            metastoreJson,
            transformMeta.getName(),
            transformMeta.getTransformPluginId(),
            transformMetaXml,
            inputRowMetaJson,
            outputRowMetaJson,
            inputTransform,
            Collections.emptyList(),
            metricsAccumulator,
            sampleDataAccumulator,
            runConfigName != null ? runConfigName : runConfigurationName,
            parentLogChannelId,
            dataSamplersJson);

    Dataset<Row> source;
    if (inputTransform) {
      // One empty row / one partition to drive source transforms (e.g. Row Generator)
      StructType emptySchema =
          new StructType()
              .add("_spark_hop_driver", org.apache.spark.sql.types.DataTypes.IntegerType, false);
      List<Row> driverRows = new ArrayList<>();
      driverRows.add(RowFactory.create(1));
      source = spark.createDataFrame(driverRows, emptySchema).repartition(1);
    } else {
      source = input;
    }

    JavaRDD<Row> outRdd =
        source
            .toJavaRDD()
            .mapPartitions(
                iterator -> {
                  try {
                    return mapFn.call(iterator);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });

    Dataset<Row> output = spark.createDataFrame(outRdd, outputSchema);
    transformDatasetMap.put(transformMeta.getName(), output);
    log.logBasic(
        "Handled transform '"
            + transformMeta.getName()
            + "' with generic Spark mapPartitions (plugin id="
            + transformMeta.getTransformPluginId()
            + ")");
  }

  private static List<SparkVariableValue> collectVariables(IVariables variables) {
    List<SparkVariableValue> list = new ArrayList<>();
    for (String name : variables.getVariableNames()) {
      list.add(new SparkVariableValue(name, variables.getVariable(name)));
    }
    return list;
  }
}
