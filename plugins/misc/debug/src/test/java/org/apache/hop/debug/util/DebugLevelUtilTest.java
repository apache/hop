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

package org.apache.hop.debug.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.debug.action.ActionDebugLevel;
import org.apache.hop.debug.transform.TransformDebugLevel;
import org.junit.jupiter.api.Test;

class DebugLevelUtilTest {

  @Test
  void storeAndLoadTransformDebugLevelWithCode() throws Exception {
    Map<String, String> map = new HashMap<>();
    TransformDebugLevel level = new TransformDebugLevel(LogLevel.DEBUG);
    level.setStartRow(1);
    level.setEndRow(10);

    DebugLevelUtil.storeTransformDebugLevel(map, "MyTransform", level);
    TransformDebugLevel loaded = DebugLevelUtil.getTransformDebugLevel(map, "MyTransform");

    assertNotNull(loaded);
    assertEquals(LogLevel.DEBUG.getCode(), loaded.getLogLevel());
    assertEquals(1, loaded.getStartRow());
    assertEquals(10, loaded.getEndRow());
  }

  @Test
  void storeAndLoadTransformDebugLevelWithVariable() throws Exception {
    Map<String, String> map = new HashMap<>();
    TransformDebugLevel level = new TransformDebugLevel("${MY_LOG_LEVEL}");

    DebugLevelUtil.storeTransformDebugLevel(map, "MyTransform", level);
    TransformDebugLevel loaded = DebugLevelUtil.getTransformDebugLevel(map, "MyTransform");

    assertNotNull(loaded);
    assertEquals("${MY_LOG_LEVEL}", loaded.getLogLevel());
  }

  @Test
  void storeAndLoadActionDebugLevelWithVariable() {
    Map<String, String> map = new HashMap<>();
    ActionDebugLevel level = new ActionDebugLevel("${ACTION_LEVEL}");
    level.setLoggingResult(true);

    DebugLevelUtil.storeActionDebugLevel(map, "MyAction", level);
    ActionDebugLevel loaded = DebugLevelUtil.getActionDebugLevel(map, "MyAction");

    assertNotNull(loaded);
    assertEquals("${ACTION_LEVEL}", loaded.getLogLevel());
    assertEquals(true, loaded.isLoggingResult());
  }

  @Test
  void getDebugLevelReturnsNullWhenMissing() throws Exception {
    Map<String, String> map = new HashMap<>();
    assertNull(DebugLevelUtil.getTransformDebugLevel(map, "Missing"));
    assertNull(DebugLevelUtil.getActionDebugLevel(map, "Missing"));
  }

  @Test
  void resolveLogLevelFromCode() {
    Variables variables = new Variables();
    assertEquals(LogLevel.ROWLEVEL, DebugLevelUtil.resolveLogLevel(variables, "Rowlevel"));
    assertEquals(LogLevel.DEBUG, DebugLevelUtil.resolveLogLevel(variables, "Debug"));
  }

  @Test
  void resolveLogLevelFromVariable() {
    Variables variables = new Variables();
    variables.setVariable("MY_LEVEL", "Detailed");
    assertEquals(LogLevel.DETAILED, DebugLevelUtil.resolveLogLevel(variables, "${MY_LEVEL}"));
  }

  @Test
  void resolveLogLevelFromDescription() {
    Variables variables = new Variables();
    String description = LogLevel.MINIMAL.getDescription();
    assertEquals(LogLevel.MINIMAL, DebugLevelUtil.resolveLogLevel(variables, description));
  }

  @Test
  void resolveLogLevelUnknownFallsBackToBasic() {
    Variables variables = new Variables();
    assertEquals(LogLevel.BASIC, DebugLevelUtil.resolveLogLevel(variables, "not-a-level"));
    assertEquals(LogLevel.BASIC, DebugLevelUtil.resolveLogLevel(variables, ""));
    assertEquals(LogLevel.BASIC, DebugLevelUtil.resolveLogLevel(null, null));
  }

  @Test
  void logLevelCodeToDisplayMapsCodeToDescription() {
    assertEquals(LogLevel.DEBUG.getDescription(), DebugLevelUtil.logLevelCodeToDisplay("Debug"));
    assertEquals("${MY_LEVEL}", DebugLevelUtil.logLevelCodeToDisplay("${MY_LEVEL}"));
  }

  @Test
  void logLevelDisplayToCodeMapsDescriptionToCode() {
    assertEquals(
        LogLevel.DEBUG.getCode(),
        DebugLevelUtil.logLevelDisplayToCode(LogLevel.DEBUG.getDescription()));
    assertEquals("${MY_LEVEL}", DebugLevelUtil.logLevelDisplayToCode("${MY_LEVEL}"));
    assertEquals("Debug", DebugLevelUtil.logLevelDisplayToCode("Debug"));
  }
}
