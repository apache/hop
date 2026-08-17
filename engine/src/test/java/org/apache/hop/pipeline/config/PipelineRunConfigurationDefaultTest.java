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

package org.apache.hop.pipeline.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;

/** Ensures at most one pipeline run configuration keeps the default flag (issue #2753). */
class PipelineRunConfigurationDefaultTest {

  @Test
  void clearDefaultFlagFromOthersClearsOtherDefaults() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    IHopMetadataSerializer<PipelineRunConfiguration> serializer =
        metadataProvider.getSerializer(PipelineRunConfiguration.class);

    serializer.save(config("A", true));
    serializer.save(config("B", true));
    serializer.save(config("C", false));

    PipelineRunConfiguration.clearDefaultFlagFromOthers(metadataProvider, "B");

    assertFalse(serializer.load("A").isDefaultSelection());
    assertTrue(serializer.load("B").isDefaultSelection());
    assertFalse(serializer.load("C").isDefaultSelection());
  }

  @Test
  void clearDefaultFlagFromOthersLeavesMatchingDefault() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    IHopMetadataSerializer<PipelineRunConfiguration> serializer =
        metadataProvider.getSerializer(PipelineRunConfiguration.class);

    serializer.save(config("only-default", true));

    PipelineRunConfiguration.clearDefaultFlagFromOthers(metadataProvider, "only-default");

    assertTrue(serializer.load("only-default").isDefaultSelection());
  }

  @Test
  void clearDefaultFlagFromOthersDoesNotTouchNonDefaults() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    IHopMetadataSerializer<PipelineRunConfiguration> serializer =
        metadataProvider.getSerializer(PipelineRunConfiguration.class);

    serializer.save(config("A", false));
    serializer.save(config("B", false));

    PipelineRunConfiguration.clearDefaultFlagFromOthers(metadataProvider, "B");

    assertFalse(serializer.load("A").isDefaultSelection());
    assertFalse(serializer.load("B").isDefaultSelection());
  }

  private static PipelineRunConfiguration config(String name, boolean defaultSelection) {
    PipelineRunConfiguration configuration = new PipelineRunConfiguration();
    configuration.setName(name);
    configuration.setDefaultSelection(defaultSelection);
    return configuration;
  }
}
