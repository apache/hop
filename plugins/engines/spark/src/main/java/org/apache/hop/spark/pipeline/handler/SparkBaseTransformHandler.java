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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.HopSparkUtil;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.core.SparkNativeMetricsListener;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.pipeline.ISparkPipelineTransformHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class SparkBaseTransformHandler implements ISparkPipelineTransformHandler {

  private SparkTransformMetricsAccumulator metricsAccumulator;
  private SparkNativeMetricsListener metricsListener;

  public void setMetricsAccumulator(SparkTransformMetricsAccumulator metricsAccumulator) {
    this.metricsAccumulator = metricsAccumulator;
  }

  protected SparkTransformMetricsAccumulator getMetricsAccumulator() {
    return metricsAccumulator;
  }

  public void setMetricsListener(SparkNativeMetricsListener metricsListener) {
    this.metricsListener = metricsListener;
  }

  /**
   * Attach native row tracking for this transform when a metrics accumulator is registered. Returns
   * {@code dataset} unchanged when metrics are disabled.
   */
  protected Dataset<Row> trackMetrics(
      Dataset<Row> dataset, TransformMeta transformMeta, SparkNativeMetrics.Role role) {
    if (dataset == null || transformMeta == null) {
      return dataset;
    }
    return SparkNativeMetrics.track(dataset, transformMeta.getName(), metricsListener, role);
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  /**
   * Loads a detached copy of this transform's metadata from its XML snapshot (never mutating the
   * live pipeline instance) and resolves info/target transforms against the pipeline.
   *
   * @see HopSparkUtil#loadTransformMetadata(ITransformMeta, TransformMeta, IHopMetadataProvider,
   *     PipelineMeta)
   */
  protected void loadTransformMetadata(
      ITransformMeta meta,
      TransformMeta transformMeta,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta)
      throws HopException {
    HopSparkUtil.loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);
  }
}
