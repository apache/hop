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
package org.apache.hop.pipeline.transforms.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class JaninoCheckerUtilTest {

  @BeforeAll
  static void init() {
    HopLogStore.init();
  }

  // ------------------------------------------------------------------ constructor

  @Test
  void constructor_noExclusionsFile_doesNotThrow() {
    // The codeExclusions.xml file will not be found in test env → logged, no throw
    assertDoesNotThrow(JaninoCheckerUtil::new);
  }

  @Test
  void constructor_whenFileNotFound_matchesListIsEmpty() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    // No exclusions file in test classpath → empty matches
    assertTrue(util.matches.isEmpty());
  }

  // ------------------------------------------------------------------ checkCode: no exclusions

  @Test
  void checkCode_noExclusions_alwaysReturnsEmpty() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    assertTrue(util.checkCode("System.exit(0);").isEmpty());
    assertTrue(util.checkCode("Runtime.getRuntime().exec(\"cmd\")").isEmpty());
    assertTrue(util.checkCode("").isEmpty());
  }

  // ------------------------------------------------------------------ checkCode: with exclusions

  @Test
  void checkCode_matchingExclusion_returnsIt() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    util.matches.add("System.exit");

    List<String> violations = util.checkCode("System.exit(0);");
    assertEquals(1, violations.size());
    assertEquals("System.exit", violations.get(0));
  }

  @Test
  void checkCode_noMatchingExclusion_returnsEmpty() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    util.matches.add("System.exit");

    assertTrue(util.checkCode("Math.abs(-1)").isEmpty());
  }

  @Test
  void checkCode_multipleExclusions_onlySomeMatch() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    util.matches.add("System.exit");
    util.matches.add("Runtime.exec");
    util.matches.add("ProcessBuilder");

    List<String> violations = util.checkCode("Runtime.exec(\"cmd\")");
    assertEquals(1, violations.size());
    assertEquals("Runtime.exec", violations.get(0));
  }

  @Test
  void checkCode_multipleExclusionsAllMatch_returnsAll() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    util.matches.add("System.exit");
    util.matches.add("Runtime.exec");

    List<String> violations = util.checkCode("System.exit(0); Runtime.exec(\"cmd\")");
    assertEquals(2, violations.size());
    assertTrue(violations.contains("System.exit"));
    assertTrue(violations.contains("Runtime.exec"));
  }

  @Test
  void checkCode_partialMatch_returnsMatch() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    util.matches.add("exit");

    // "exit" appears as a substring within "System.exit"
    assertFalse(util.checkCode("System.exit(1)").isEmpty());
  }

  // ------------------------------------------------------------------ getJarPath

  @Test
  void getJarPath_returnsNonNullNonEmpty() {
    JaninoCheckerUtil util = new JaninoCheckerUtil();
    String path = util.getJarPath();
    assertNotNull(path);
    assertFalse(path.isBlank());
  }
}
