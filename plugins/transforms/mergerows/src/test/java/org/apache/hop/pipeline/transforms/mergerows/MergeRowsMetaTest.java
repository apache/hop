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
 *
 */

package org.apache.hop.pipeline.transforms.mergerows;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MergeRowsMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    MergeRowsMeta meta =
        TransformSerializationTestUtil.testSerialization("/merge-rows.xml", MergeRowsMeta.class);

    assertEquals(1, meta.getKeyFields().size());
    assertEquals("id", meta.getKeyFields().get(0));
    assertEquals("flagfield", meta.getFlagField());
    assertEquals("source1", meta.getReferenceTransform());
    assertEquals("source2", meta.getCompareTransform());
    assertEquals(2, meta.getValueFields().size());
    assertEquals("name", meta.getValueFields().get(0));
    assertEquals("score", meta.getValueFields().get(1));
  }
}
