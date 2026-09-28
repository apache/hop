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

package org.apache.hop.pipeline.transforms.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.junit.jupiter.api.Test;

class SimpleMappingExecutionInfoTest {

  @Test
  void singleThreadedMappingDropsTheChildExecutionInformationLocation() {
    LocalPipelineEngine engine = new LocalPipelineEngine();
    PipelineRunConfiguration original = engine.getPipelineRunConfiguration();
    original.setName("with-location");
    original.setExecutionInfoLocationName("caching-db");
    original.setExecutionDataProfileName("first-rows");

    assertTrue(SimpleMapping.suppressExecutionInformation(engine));

    PipelineRunConfiguration used = engine.getPipelineRunConfiguration();
    assertNull(used.getExecutionInfoLocationName());
    assertNull(used.getExecutionDataProfileName());
    assertEquals("with-location", used.getName());
    assertEquals("caching-db", original.getExecutionInfoLocationName());
    assertEquals("first-rows", original.getExecutionDataProfileName());
    assertNotSame(original, used);
  }

  @Test
  void mappingWithoutALocationKeepsItsRunConfiguration() {
    LocalPipelineEngine engine = new LocalPipelineEngine();
    PipelineRunConfiguration original = engine.getPipelineRunConfiguration();

    assertFalse(SimpleMapping.suppressExecutionInformation(engine));
    assertSame(original, engine.getPipelineRunConfiguration());
  }
}
