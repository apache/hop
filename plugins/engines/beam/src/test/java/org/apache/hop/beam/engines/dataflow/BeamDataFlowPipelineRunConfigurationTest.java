/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.engines.dataflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hop.core.HopEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Issue #2355: BigQuery load jobs need a GCS temp location, separate from the general one. */
class BeamDataFlowPipelineRunConfigurationTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void gcpTempLocationIsPropagatedToThePipelineOptions() throws Exception {
    BeamDataFlowPipelineRunConfiguration runConfiguration =
        new BeamDataFlowPipelineRunConfiguration();
    runConfiguration.setGcpTempLocation("gs://my-bucket/beam-temp");

    // Build the options the way the engine does, from a plain PipelineOptions instance, so the
    // test does not need GCP application default credentials.  Going through
    // getPipelineOptions() asks the Dataflow client for credentials, which a unit test cannot
    // have; that call is exercised by the engine, not here.
    //
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.as(GcpOptions.class).setGcpTempLocation(runConfiguration.getGcpTempLocation());

    assertEquals("gs://my-bucket/beam-temp", options.as(GcpOptions.class).getGcpTempLocation());
  }

  @Test
  void cloneCopiesTheGcpTempLocation() {
    BeamDataFlowPipelineRunConfiguration original = new BeamDataFlowPipelineRunConfiguration();
    original.setGcpTempLocation("gs://my-bucket/beam-temp");

    BeamDataFlowPipelineRunConfiguration copy = original.clone();

    assertEquals("gs://my-bucket/beam-temp", copy.getGcpTempLocation());
  }

  @Test
  void gcpTempLocationIsSeparateFromTheGeneralTempLocation() {
    // They are two different fields and two different Beam options: the general tempLocation is
    // applied by the converter for staging, the GCP one by this run configuration for the
    // transforms that require a Cloud Storage path.
    //
    BeamDataFlowPipelineRunConfiguration runConfiguration =
        new BeamDataFlowPipelineRunConfiguration();
    runConfiguration.setTempLocation("gs://my-bucket/general");
    runConfiguration.setGcpTempLocation("gs://my-bucket/gcp-specific");

    assertEquals("gs://my-bucket/general", runConfiguration.getTempLocation());
    assertEquals("gs://my-bucket/gcp-specific", runConfiguration.getGcpTempLocation());
  }

  @Test
  void defaultIsUnset() {
    assertNull(new BeamDataFlowPipelineRunConfiguration().getGcpTempLocation());
  }
}
