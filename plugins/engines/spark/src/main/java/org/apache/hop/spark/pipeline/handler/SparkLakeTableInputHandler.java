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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.table.SparkLakeTableSupport;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Native Spark lake table read (Delta PATH in v1 / PR 2). */
public class SparkLakeTableInputHandler extends SparkBaseTransformHandler {

  @Override
  public boolean isInput() {
    return true;
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

    SparkLakeTableInputMeta meta = new SparkLakeTableInputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    Dataset<Row> dataset =
        SparkLakeTableSupport.resolveRead(spark, variables, log, transformMeta.getName(), meta);
    dataset = trackMetrics(dataset, transformMeta, SparkNativeMetrics.Role.INPUT);
    transformDatasetMap.put(transformMeta.getName(), dataset);

    String target =
        SparkLakeTableInputMeta.MODE_TABLE.equalsIgnoreCase(
                String.valueOf(meta.getIdentifierMode()))
            ? "table=" + variables.resolve(meta.getTableIdentifier())
            : "path=" + variables.resolve(meta.getTablePath());
    log.logBasic(
        "Handled Spark Lake Table Input : "
            + transformMeta.getName()
            + " format="
            + meta.getFormat()
            + " mode="
            + meta.getIdentifierMode()
            + " "
            + target
            + " timeTravel="
            + meta.getTimeTravelType()
            + " columns="
            + Arrays.toString(dataset.columns()));
  }
}
