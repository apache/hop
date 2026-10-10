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

package org.apache.hop.beam.engines.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.Map;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.jupiter.api.Test;

/** Issue #2355: DirectRunner needs the same GCS temp location as Dataflow. */
class BeamDirectPipelineRunConfigurationTest {

  @Test
  void gcpTempLocationIsResolvedOntoGcpOptions() throws Exception {
    var runConfiguration = new BeamDirectPipelineRunConfiguration();
    runConfiguration.setGcpTempLocation("${GCP_TEMP}");
    runConfiguration.setVariable("GCP_TEMP", "gs://my-bucket/beam-temp");

    DirectOptions options = (DirectOptions) runConfiguration.getPipelineOptions();

    assertEquals("gs://my-bucket/beam-temp", options.as(GcpOptions.class).getGcpTempLocation());
    assertTrue(explicitlySet(options, "gcpTempLocation"));
    assertTrue(runConfiguration.getTempLocation().startsWith("file://"));
  }

  @Test
  void emptyGcpTempLocationLeavesTheOptionUnset() throws Exception {
    var runConfiguration = new BeamDirectPipelineRunConfiguration();
    assertNull(runConfiguration.getGcpTempLocation());
    DirectOptions options = (DirectOptions) runConfiguration.getPipelineOptions();
    // Reading GcpOptions.getGcpTempLocation() runs its default factory, which contacts Cloud
    // Storage. An empty Hop field must not put the option in the explicit map.
    assertFalse(explicitlySet(options, "gcpTempLocation"));
  }

  @Test
  void cloneCopiesTheGcpTempLocationAndLeavesTheGeneralTempLocation() {
    var original = new BeamDirectPipelineRunConfiguration();
    original.setTempLocation("file:///tmp/hop");
    original.setGcpTempLocation("gs://my-bucket/gcp-specific");

    BeamDirectPipelineRunConfiguration copy = original.clone();

    assertEquals("file:///tmp/hop", copy.getTempLocation());
    assertEquals("gs://my-bucket/gcp-specific", copy.getGcpTempLocation());
    assertNull(new BeamDirectPipelineRunConfiguration().getGcpTempLocation());
  }

  @SuppressWarnings("unchecked")
  private static boolean explicitlySet(PipelineOptions options, String name) throws Exception {
    var handler = Proxy.getInvocationHandler(options);
    var field = handler.getClass().getDeclaredField("options");
    field.setAccessible(true);
    return ((Map<String, ?>) field.get(handler)).containsKey(name);
  }
}
