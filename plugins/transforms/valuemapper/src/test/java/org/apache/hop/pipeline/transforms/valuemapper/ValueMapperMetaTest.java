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
package org.apache.hop.pipeline.transforms.valuemapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValueMapperMetaTest {

  @BeforeEach
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  public void testSerialization() throws Exception {
    ValueMapperMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/value-mapper-transform.xml", ValueMapperMeta.class);

    assertEquals(7, meta.getValues().size());
    // Test serialization with null source attribute
    assertNull(meta.getValues().get(0).getSource());
    assertEquals("[${NOT_DEFINED}]", meta.getValues().get(0).getTarget());

    assertEquals("BE", meta.getValues().get(1).getSource());
    assertEquals("Belgium", meta.getValues().get(1).getTarget());

    assertEquals("Country_Code", meta.getFieldToUse());
    assertEquals("Country_Name", meta.getTargetField());
    assertEquals("[${NOT_FOUND}]", meta.getNonMatchDefault());
  }
}
