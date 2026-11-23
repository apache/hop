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

package org.apache.hop.beam.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.hop.beam.engines.dataflow.BeamDataFlowPipelineEngine;
import org.apache.hop.beam.engines.dataflow.BeamDataFlowPipelineRunConfiguration;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineRunConfiguration;
import org.apache.hop.beam.engines.flink.BeamFlinkPipelineEngine;
import org.apache.hop.beam.engines.flink.BeamFlinkPipelineRunConfiguration;
import org.apache.hop.beam.engines.spark.BeamSparkPipelineEngine;
import org.apache.hop.beam.engines.spark.BeamSparkPipelineRunConfiguration;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.junit.jupiter.api.Test;

/** A run configuration created in code names its engine, so it can be saved and read back. */
class BeamPipelineRunConfigurationIdTest {

  @Test
  void eachDefaultRunConfigurationNamesItsEngine() {
    Map<Class<?>, BeamPipelineRunConfiguration> configurations =
        Map.of(
            BeamDirectPipelineEngine.class, new BeamDirectPipelineRunConfiguration(),
            BeamDataFlowPipelineEngine.class, new BeamDataFlowPipelineRunConfiguration(),
            BeamFlinkPipelineEngine.class, new BeamFlinkPipelineRunConfiguration(),
            BeamSparkPipelineEngine.class, new BeamSparkPipelineRunConfiguration());

    configurations.forEach(
        (engineClass, configuration) ->
            assertEquals(
                engineClass.getAnnotation(PipelineEnginePlugin.class).id(),
                configuration.getEnginePluginId(),
                engineClass.getSimpleName()));
  }
}
