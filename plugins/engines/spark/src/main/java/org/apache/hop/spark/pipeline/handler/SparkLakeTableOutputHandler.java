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
import org.apache.hop.spark.table.SparkLakeActionSupport;
import org.apache.hop.spark.table.SparkLakeTableSupport;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableOutputMeta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Native Spark lake table write (Delta PATH in v1 / PR 2). Executes the write as an action and
 * registers an empty leaf Dataset.
 */
public class SparkLakeTableOutputHandler extends SparkBaseTransformHandler {

  @Override
  public boolean isOutput() {
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

    if (input == null) {
      throw new HopException(
          "Spark Lake Table Output '"
              + transformMeta.getName()
              + "' has no input Dataset. Connect an enabled hop from an upstream transform"
              + " (disabled hops and disconnected transforms are not executed on native Spark).");
    }

    SparkLakeTableOutputMeta meta = new SparkLakeTableOutputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    Dataset<Row> toWrite = trackMetrics(input, transformMeta, SparkNativeMetrics.Role.OUTPUT);
    SparkLakeTableSupport.resolveWrite(spark, toWrite, variables, transformMeta.getName(), meta);
    SparkLakeActionSupport.putEmptyLeaf(transformDatasetMap, transformMeta.getName(), spark);

    String target =
        SparkLakeTableInputMeta.MODE_TABLE.equalsIgnoreCase(
                String.valueOf(meta.getIdentifierMode()))
            ? "table=" + variables.resolve(meta.getTableIdentifier())
            : "path=" + variables.resolve(meta.getTablePath());
    log.logBasic(
        "Handled Spark Lake Table Output : "
            + transformMeta.getName()
            + " format="
            + meta.getFormat()
            + " mode="
            + meta.getIdentifierMode()
            + " saveMode="
            + meta.getSaveMode()
            + " "
            + target);
  }
}
