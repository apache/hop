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

package org.apache.hop.pipeline.transforms.nullif;

import static org.junit.Assert.assertEquals;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class NullIfMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  public void testSerialization() throws Exception {
    NullIfMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/null-if-transform.xml", NullIfMeta.class);
    
    assertEquals(2, meta.getFields().size());
    assertEquals("fieldName", meta.getFields().get(0).getName());
    assertEquals("two", meta.getFields().get(0).getValue());    
  }
}
