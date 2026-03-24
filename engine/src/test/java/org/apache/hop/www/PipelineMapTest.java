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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PipelineMapTest {

  private PipelineMap map;

  @BeforeEach
  void setUp() {
    map = new PipelineMap();
  }

  @Test
  void addGetRemoveAndList() {
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    PipelineMeta meta = mock(PipelineMeta.class);
    when(pipeline.getPipelineMeta()).thenReturn(meta);
    when(meta.getName()).thenReturn("P1");
    PipelineConfiguration configuration = mock(PipelineConfiguration.class);

    map.addPipeline("P1", "cid-1", pipeline, configuration);

    HopServerObjectEntry entry = new HopServerObjectEntry("P1", "cid-1");
    assertSame(pipeline, map.getPipeline(entry));
    assertSame(pipeline, map.getPipeline("P1"));
    assertSame(configuration, map.getConfiguration(entry));
    assertSame(configuration, map.getConfiguration("P1"));

    List<HopServerObjectEntry> objects = map.getPipelineObjects();
    assertEquals(1, objects.size());
    assertEquals(entry.getId(), objects.get(0).getId());

    assertNotNull(map.getFirstServerObjectEntry("P1"));

    map.removePipeline(entry);
    assertNull(map.getPipeline(entry));
    assertTrue(map.getPipelineObjects().isEmpty());
  }

  @Test
  void getConfigurationReturnsNullForUnknownEntry() {
    assertNull(map.getConfiguration(new HopServerObjectEntry("x", "y")));
  }

  @Test
  void hopServerConfigRoundTrip() {
    HopServerConfig cfg = mock(HopServerConfig.class);
    map.setHopServerConfig(cfg);
    assertSame(cfg, map.getHopServerConfig());
  }
}
