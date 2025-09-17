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

package org.apache.hop.pipeline.transforms.javascript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.mozilla.javascript.EvaluatorException;

class ScriptValueAddFunctions_GetVariableScopeTest {

  @Test
  void getSystemVariableScope() {
    assertEquals(
        ScriptValuesAddedFunctions.VariableScope.SYSTEM,
        ScriptValuesAddedFunctions.getVariableScope("s"));
  }

  @Test
  void getRootVariableScope() {
    assertEquals(
        ScriptValuesAddedFunctions.VariableScope.ROOT,
        ScriptValuesAddedFunctions.getVariableScope("r"));
  }

  @Test
  void getParentVariableScope() {
    assertEquals(
        ScriptValuesAddedFunctions.VariableScope.PARENT,
        ScriptValuesAddedFunctions.getVariableScope("p"));
  }

  @Test
  void getGrandParentVariableScope() {
    assertEquals(
        ScriptValuesAddedFunctions.VariableScope.GRAND_PARENT,
        ScriptValuesAddedFunctions.getVariableScope("g"));
  }

  @Test
  void getNonExistingVariableScope() {
    assertThrows(
        EvaluatorException.class, () -> ScriptValuesAddedFunctions.getVariableScope("dummy"));
  }
}
