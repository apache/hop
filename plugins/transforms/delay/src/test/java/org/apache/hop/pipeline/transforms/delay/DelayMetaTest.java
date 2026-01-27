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

package org.apache.hop.pipeline.transforms.delay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Unit test for {@link DelayMeta} */
class DelayMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private DelayMeta meta;

  @BeforeEach
  void setUp() {
    meta = new DelayMeta();
  }

  @Test
  void testTransformMeta() throws HopException {
    List<String> attributes =
        Arrays.asList(
            "timeout", "timeoutField", "scaletime", "scaleTimeFromField", "scaleTimeField");

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("timeout", "getTimeout");
    getterMap.put("timeoutField", "getTimeoutField");
    getterMap.put("scaletime", "getScaletime");
    getterMap.put("scaleTimeFromField", "isScaleTimeFromField");
    getterMap.put("scaleTimeField", "getScaleTimeField");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("timeout", "setTimeout");
    setterMap.put("timeoutField", "setTimeoutField");
    setterMap.put("scaletime", "setScaletime");
    setterMap.put("scaleTimeFromField", "setScaleTimeFromField");
    setterMap.put("scaleTimeField", "setScaleTimeField");

    LoadSaveTester<DelayMeta> loadSaveTester =
        new LoadSaveTester<>(DelayMeta.class, attributes, getterMap, setterMap);
    loadSaveTester.testSerialization();
  }

  @Test
  void testSetDefault() {
    meta.setDefault();

    assertEquals("1", meta.getTimeout());
    assertEquals("seconds", meta.getScaletime());
    assertNull(meta.getTimeoutField());
    assertFalse(meta.isScaleTimeFromField());
    assertNull(meta.getScaleTimeField());
  }

  @ParameterizedTest(name = "index={0} <-> scaleTime={1}")
  @CsvSource({"0, milliseconds", "1, seconds", "2, minutes", "3, hours"})
  void testSetAndGetScaleTimeCode_Normal(int index, String expected) {
    meta.setScaleTimeCode(index);

    assertEquals(expected, meta.getScaletime());
    assertEquals(index, meta.getScaleTimeCode());
  }

  @Test
  void testGetScaleTimeCodeWhenNull() {
    meta.setScaletime(null);
    assertEquals(1, meta.getScaleTimeCode());
  }

  @Test
  void testSetTimeoutFieldEmpty() {
    meta.setTimeoutField("");
    assertNull(meta.getTimeoutField());
  }

  @Test
  void testSetTimeoutFieldValue() {
    meta.setTimeoutField("timeout_col");
    assertEquals("timeout_col", meta.getTimeoutField());
  }

  @Test
  void testSetScaleTimeFieldEmpty() {
    meta.setScaleTimeField("  ");
    assertNotNull(meta.getScaleTimeField());
  }

  @Test
  void testSetScaleTimeFieldValue() {
    meta.setScaleTimeField("scale_col");
    assertEquals("scale_col", meta.getScaleTimeField());
  }
}
