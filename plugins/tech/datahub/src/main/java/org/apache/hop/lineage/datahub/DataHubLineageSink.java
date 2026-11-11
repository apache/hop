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

package org.apache.hop.lineage.datahub;

import java.util.Locale;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.openlineage.OpenLineageEmitter;
import org.apache.hop.lineage.openlineage.OpenLineageEventMapper;
import org.apache.hop.lineage.openlineage.OpenLineageLineageSink;
import org.apache.hop.lineage.openlineage.OpenLineageSinkConfig;
import org.apache.hop.lineage.plugin.LineageSinkPlugin;

/**
 * Hop lineage sink that writes pipelines, workflows, their runs and the datasets they touch to
 * DataHub.
 *
 * <p>It shares the OpenLineage sink's mapping, asynchronous queue and spool, and writes the mapped
 * events to DataHub's metadata API instead of posting them to an OpenLineage collector; see {@link
 * DataHubEmitter} for why, and {@link DataHubTranslator} for the model. Enable it with {@code
 * HOP_LINEAGE_ENABLED=Y}, {@code HOP_LINEAGE_SINK_IDS=datahub} and {@code HOP_LINEAGE_DATAHUB_URL}.
 */
@LineageSinkPlugin(
    id = "datahub",
    name = "DataHub",
    description = "Writes Hop pipelines, workflows, runs and dataset lineage to DataHub")
public class DataHubLineageSink extends OpenLineageLineageSink {

  private DataHubSinkConfig dataHubConfig;

  @Override
  protected String sinkName() {
    return "DataHub";
  }

  @Override
  protected OpenLineageSinkConfig readConfig(IVariables variables) throws HopException {
    try {
      dataHubConfig = DataHubSinkConfig.fromVariables(variables);
      return dataHubConfig.toOpenLineageConfig();
    } catch (IllegalArgumentException e) {
      throw new HopException(e.getMessage(), e);
    }
  }

  /**
   * Keeps this sink's spool folders apart from the OpenLineage sink's, should both use one folder.
   */
  @Override
  protected String spoolPrefix() {
    return "datahub-";
  }

  /**
   * One job per transform holding only its own datasets: DataHub links the jobs through the hop
   * graph, so the propagated inputs an OpenLineage collector needs would only draw a second, direct
   * edge next to the path through the transforms. DataHub keeps a run's state, so a run that never
   * reported its end is ended when the sink shuts down rather than shown as running for good.
   */
  @Override
  protected void configureMapper(OpenLineageEventMapper.Builder builder) {
    builder.transformJobs(true).propagateInputs(false).parentRuns(true).abortOpenRuns(true);
  }

  @Override
  protected OpenLineageEmitter createEmitter(OpenLineageSinkConfig config, ILogChannel log)
      throws HopException {
    return new DataHubEmitter(
        new DataHubTranslator(dataHubConfig),
        new DataHubClient(dataHubConfig.getGmsUrl(), dataHubConfig.getToken(), config),
        dataHubConfig.getSchemaPolicy(),
        dataHubConfig.getActor(),
        config.getMaxRetries(),
        config.getRetryBackoffMs(),
        log);
  }

  @Override
  protected String describeTarget(OpenLineageSinkConfig config) {
    return "gms="
        + dataHubConfig.getGmsUrl()
        + ", env="
        + dataHubConfig.getEnv()
        + (dataHubConfig.getPlatformInstance() == null
            ? ""
            : ", platform instance=" + dataHubConfig.getPlatformInstance())
        + ", dataset schemas="
        + dataHubConfig.getSchemaPolicy().name().toLowerCase(Locale.ROOT)
        + ", lifecycle="
        + (dataHubConfig.isLifecycleEnabled() ? "on" : "off");
  }
}
