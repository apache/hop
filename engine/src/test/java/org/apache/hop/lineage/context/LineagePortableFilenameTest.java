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

package org.apache.hop.lineage.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class LineagePortableFilenameTest {

  @Test
  void portableKey_nullOrEmpty() {
    Variables v = new Variables();
    v.setVariable(LineagePortableFilename.VARIABLE_PROJECT_HOME, "/opt/proj");
    assertNull(LineagePortableFilename.portableKey(null, v));
    assertNull(LineagePortableFilename.portableKey("", v));
    assertNull(LineagePortableFilename.portableKey("  ", v));
    assertNull(LineagePortableFilename.portableKey("/opt/proj/a.hpl", null));
  }

  @Test
  void portableKey_preservesVariableStyle() {
    Variables v = new Variables();
    assertEquals(
        "${PROJECT_HOME}/pipelines/x.hpl",
        LineagePortableFilename.portableKey(" ${PROJECT_HOME}/pipelines/x.hpl ", v));
  }

  @Test
  void portableKey_normalizesBackslashesAfterToken() {
    Variables v = new Variables();
    assertEquals(
        "${PROJECT_HOME}/pipelines/x.hpl",
        LineagePortableFilename.portableKey("${PROJECT_HOME}\\pipelines\\x.hpl", v));
  }

  @Test
  void portableKey_fromResolvedAbsolutePathUnderProjectHome() {
    Variables v = new Variables();
    v.setVariable(LineagePortableFilename.VARIABLE_PROJECT_HOME, "/tmp/myproject");
    assertEquals(
        "${PROJECT_HOME}/transforms/t.hpl",
        LineagePortableFilename.portableKey("/tmp/myproject/transforms/t.hpl", v));
  }

  @Test
  void portableKey_outsideProjectHome_returnsNull() {
    Variables v = new Variables();
    v.setVariable(LineagePortableFilename.VARIABLE_PROJECT_HOME, "/tmp/myproject");
    assertNull(LineagePortableFilename.portableKey("/other/read.hpl", v));
  }

  @Test
  void portableKey_unsetProjectHome_returnsNullForAbsolute() {
    Variables v = new Variables();
    assertNull(LineagePortableFilename.portableKey("/tmp/myproject/a.hpl", v));
  }
}
