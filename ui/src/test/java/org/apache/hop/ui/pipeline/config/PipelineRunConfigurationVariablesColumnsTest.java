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

package org.apache.hop.ui.pipeline.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.core.widget.ColumnInfo;
import org.junit.jupiter.api.Test;

/** Verifies Pipeline Run Configuration Variables tab only enables the picker on Value (#7604). */
class PipelineRunConfigurationVariablesColumnsTest {

  @Test
  void onlyValueColumnUsesVariables() {
    ColumnInfo[] columns =
        PipelineRunConfigurationEditor.createVariablesColumns("Name", "Value", "Description");

    assertEquals(3, columns.length);
    assertFalse(columns[0].isUsingVariables(), "variable name is a literal key");
    assertTrue(columns[1].isUsingVariables(), "value supports ${...} references");
    assertFalse(columns[2].isUsingVariables(), "description does not use variables");
  }
}
