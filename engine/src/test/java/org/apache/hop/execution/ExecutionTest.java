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
 *
 */

package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.util.Map;
import org.apache.hop.core.json.HopJson;
import org.junit.jupiter.api.Test;

class ExecutionTest {

  @Test
  void testParameterValuesNotSerialized() throws Exception {
    Execution execution = new Execution();
    execution.setName("test-execution");
    execution.setParameterValues(Map.of("dbPassword", "shouldNeverLeak"));
    execution.setVariableValues(Map.of("apiToken", "shouldAlsoNeverLeak"));

    StringWriter writer = new StringWriter();
    HopJson.newMapper().writeValue(writer, execution);
    String json = writer.toString();

    assertFalse(
        json.contains("shouldNeverLeak"),
        "parameterValues must not appear in serialized JSON, same as variableValues");
    assertFalse(json.contains("shouldAlsoNeverLeak"), "variableValues must not be serialized");
    assertFalse(json.contains("parameterValues"), "parameterValues key must not be serialized");
    assertFalse(json.contains("variableValues"), "variableValues key must not be serialized");
    assertTrue(json.contains("test-execution"), "other fields must still serialize normally");
  }
}
