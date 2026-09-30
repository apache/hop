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

package org.apache.hop.beam.transforms.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Issue #2275: the Beam window transform should support windowing on a key. */
class BeamWindowMetaTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void keyFieldRoundTripsThroughTheTransformXml() throws Exception {
    BeamWindowMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-window-keyed-transform.xml", BeamWindowMeta.class);

    assertEquals("customerId", meta.getKeyField());
    assertEquals("FIXED", meta.getWindowType());
    assertEquals("60", meta.getDuration());
  }

  @Test
  void theKeyFieldIsOptionalAndDefaultsToUnkeyed() {
    // A transform saved before #2275 has no key_field element, so the default has to keep the
    // previous behaviour: window across the whole stream.
    //
    BeamWindowMeta meta = new BeamWindowMeta();

    assertTrue(
        meta.getKeyField() == null || meta.getKeyField().isEmpty(),
        "expected no key field by default, got: " + meta.getKeyField());
  }

  @Test
  void setDefaultClearsTheKeyField() {
    BeamWindowMeta meta = new BeamWindowMeta();
    meta.setKeyField("customerId");

    meta.setDefault();

    assertTrue(
        meta.getKeyField() == null || meta.getKeyField().isEmpty(),
        "setDefault() must not leave a stale key field behind");
  }

  @Test
  void theKeyFieldIsNotEmittedAsAnOutputColumn() throws Exception {
    // The key is only used to group the rows for windowing. It must not appear in the output,
    // otherwise every downstream transform sees an unexpected extra field.
    //
    BeamWindowMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-window-keyed-transform.xml", BeamWindowMeta.class);

    org.apache.hop.core.row.IRowMeta rowMeta = new org.apache.hop.core.row.RowMeta();
    meta.getFields(
        rowMeta, "Window", null, null, new org.apache.hop.core.variables.Variables(), null);

    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      assertTrue(
          !"customerId".equalsIgnoreCase(valueMeta.getName()),
          "the key field should not be added to the output, but found: " + valueMeta.getName());
    }
  }

  @Test
  void anUnsetKeyFieldLeavesTheInputUntouched() throws Exception {
    BeamWindowMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-window-keyed-transform.xml", BeamWindowMeta.class);
    meta.setKeyField(null);

    assertNull(meta.getKeyField());
  }
}
