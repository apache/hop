/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.hop.pipeline.transforms.script;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ScriptAddedFunctionsTest {

  @Test
  void undefinedValue_isNull() {
    assertNull(ScriptAddedFunctions.undefinedValue);
  }

  @Test
  void functionTypeConstants_haveExpectedValues() {
    assertEquals(0, ScriptAddedFunctions.STRING_FUNCTION);
    assertEquals(1, ScriptAddedFunctions.NUMERIC_FUNCTION);
    assertEquals(2, ScriptAddedFunctions.DATE_FUNCTION);
    assertEquals(3, ScriptAddedFunctions.LOGIC_FUNCTION);
    assertEquals(4, ScriptAddedFunctions.SPECIAL_FUNCTION);
    assertEquals(5, ScriptAddedFunctions.FILE_FUNCTION);
  }

  @Test
  void jsFunctionList_isEmptyByDefault() {
    assertNotNull(ScriptAddedFunctions.jsFunctionList);
    assertEquals(0, ScriptAddedFunctions.jsFunctionList.length);
  }
}
