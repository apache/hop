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

package org.apache.hop.pipeline.transforms.cube;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class CubeFilenameTest {

  @Test
  void insertCopyNumberBeforeTheExtension() {
    Variables variables = new Variables();

    assertEquals(
        "/tmp/dir.v1/data_1.cube",
        CubeFilename.resolve(variables, "/tmp/dir.v1/data.cube", true, 1));
    assertEquals(
        "C:\\data\\file_2.cube", CubeFilename.resolve(variables, "C:\\data\\file.cube", true, 2));
    assertEquals("/tmp/data_0", CubeFilename.resolve(variables, "/tmp/data", true, 0));
  }

  @Test
  void copyVariableReplacesAStaleValueAndANestedVariable() {
    Variables variables = new Variables();
    variables.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR, "9");
    variables.setVariable("FILE", "/tmp/nested_${Internal.Transform.CopyNr}.cube");

    assertEquals(
        "/tmp/direct_0.cube",
        CubeFilename.resolve(variables, "/tmp/direct_${Internal.Transform.CopyNr}.cube", false, 0));
    assertEquals("/tmp/nested_0.cube", CubeFilename.resolve(variables, "${FILE}", false, 0));
    assertEquals(
        "/tmp/case_3.cube",
        CubeFilename.resolve(variables, "/tmp/case_${internal.transform.copynr}.cube", false, 3));
    assertEquals(
        "/tmp/win_4.cube",
        CubeFilename.resolve(variables, "/tmp/win_%%Internal.Transform.CopyNr%%.cube", false, 4));
  }

  @Test
  void checkboxAndVariableBothInsertTheNumber() {
    assertEquals(
        "/tmp/data_1_1.cube",
        CubeFilename.resolve(
            new Variables(), "/tmp/data_${Internal.Transform.CopyNr}.cube", true, 1));
  }
}
