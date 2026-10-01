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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for RuleConfig class */
public class RuleConfigTest {

  @Test
  public void testDefaultConstructor() {
    RuleConfig config = new RuleConfig();

    assertFalse(config.isEnabled());
    assertNotNull(config.getParameters());
    assertTrue(config.getParameters().isEmpty());
  }

  @Test
  public void testSettersAndGetters() {
    RuleConfig config = new RuleConfig();

    // Test enabled flag
    config.setEnabled(true);
    assertTrue(config.isEnabled());

    config.setEnabled(false);
    assertFalse(config.isEnabled());

    // Test parameters
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("maxTransforms", 25);
    parameters.put("checkPasswords", false);
    parameters.put("customParam", "testValue");

    config.setParameters(parameters);

    assertEquals(3, config.getParameters().size());
    assertEquals(25, config.getParameters().get("maxTransforms"));
    assertEquals(false, config.getParameters().get("checkPasswords"));
    assertEquals("testValue", config.getParameters().get("customParam"));
  }

  @Test
  public void testNullParameters() {
    RuleConfig config = new RuleConfig();

    config.setParameters(null);
    assertNull(config.getParameters());
  }

  @Test
  public void testEmptyParameters() {
    RuleConfig config = new RuleConfig();

    Map<String, Object> emptyParams = new HashMap<>();
    config.setParameters(emptyParams);

    assertNotNull(config.getParameters());
    assertTrue(config.getParameters().isEmpty());
  }

  @Test
  public void testComplexParameters() {
    RuleConfig config = new RuleConfig();

    Map<String, Object> complexParams = new HashMap<>();
    complexParams.put("stringParam", "test");
    complexParams.put("intParam", 42);
    complexParams.put("booleanParam", true);
    complexParams.put("doubleParam", 3.14);
    complexParams.put("nullParam", null);

    config.setParameters(complexParams);

    assertEquals("test", config.getParameters().get("stringParam"));
    assertEquals(42, config.getParameters().get("intParam"));
    assertEquals(true, config.getParameters().get("booleanParam"));
    assertEquals(3.14, config.getParameters().get("doubleParam"));
    assertNull(config.getParameters().get("nullParam"));
  }
}
