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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Dummy (do nothing) passes its input Dataset on. As a generic transform it would cost a full
 * conversion of every row to a Hop row and back; here it only counts the rows for the metrics.
 * Several input hops are combined like any other transform's.
 */
public class SparkDummyHandler extends SparkBaseTransformHandler {

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
    List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transformMeta);
    if (nextTransforms.size() > 1 && transformMeta.isDistributes()) {
      throw new HopException(
          "Transform '"
              + transformMeta.getName()
              + "' distributes its rows over "
              + nextTransforms.size()
              + " outgoing hops (round robin), which the native Spark engine does not support."
              + " Copy the rows to every hop instead (data movement: copy). plugin id="
              + transformMeta.getTransformPluginId());
    }
    Dataset<Row> output = input != null ? input : spark.emptyDataFrame();
    transformDatasetMap.put(
        transformMeta.getName(),
        trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM));
    log.logBasic("Handled transform '" + transformMeta.getName() + "' natively: rows passed on");
  }
}
