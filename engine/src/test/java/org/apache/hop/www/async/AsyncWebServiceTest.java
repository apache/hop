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

package org.apache.hop.www.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

/** Test class for AsyncWebService */
public class AsyncWebServiceTest {

  @Test
  public void testDefaultConstructor() {
    AsyncWebService webService = new AsyncWebService();

    assertTrue(webService.isEnabled());
    assertEquals("ASYNC_CONTENT", webService.getBodyContentVariable());
  }

  @Test
  public void testParameterizedConstructor() {
    String name = "test-service";
    boolean enabled = false;
    String filename = "/path/to/workflow.hwf";
    String statusVariables = "VAR1,VAR2,VAR3";
    String bodyContentVariable = "CUSTOM_CONTENT";
    String runConfigurationName = "test-run-config";
    String headerContentVariable = "HEADER_CONTENT";

    AsyncWebService webService =
        new AsyncWebService(
            name,
            enabled,
            filename,
            statusVariables,
            bodyContentVariable,
            runConfigurationName,
            headerContentVariable);

    assertEquals(name, webService.getName());
    assertEquals(enabled, webService.isEnabled());
    assertEquals(filename, webService.getFilename());
    assertEquals(statusVariables, webService.getStatusVariables());
    assertEquals(bodyContentVariable, webService.getBodyContentVariable());
    assertEquals(runConfigurationName, webService.getRunConfigurationName());
    assertEquals(headerContentVariable, webService.getHeaderContentVariable());
  }

  @Test
  public void testSettersAndGetters() {
    AsyncWebService webService = new AsyncWebService();

    webService.setEnabled(false);
    assertFalse(webService.isEnabled());
    webService.setEnabled(true);
    assertTrue(webService.isEnabled());

    String filename = "/test/workflow.hwf";
    webService.setFilename(filename);
    assertEquals(filename, webService.getFilename());

    String statusVariables = "STATUS_VAR1,STATUS_VAR2";
    webService.setStatusVariables(statusVariables);
    assertEquals(statusVariables, webService.getStatusVariables());

    String bodyContentVariable = "BODY_VAR";
    webService.setBodyContentVariable(bodyContentVariable);
    assertEquals(bodyContentVariable, webService.getBodyContentVariable());

    String runConfigurationName = "RUN_CONFIG";
    webService.setRunConfigurationName(runConfigurationName);
    assertEquals(runConfigurationName, webService.getRunConfigurationName());

    String headerContentVariable = "HEADER_VAR";
    webService.setHeaderContentVariable(headerContentVariable);
    assertEquals(headerContentVariable, webService.getHeaderContentVariable());
  }

  @Test
  public void testGetStatusVariablesList() {
    AsyncWebService webService = new AsyncWebService();
    Variables variables = new Variables();

    webService.setStatusVariables("");
    List<String> emptyList = webService.getStatusVariablesList(variables);
    assertTrue(emptyList.isEmpty());

    webService.setStatusVariables(null);
    List<String> nullList = webService.getStatusVariablesList(variables);
    assertTrue(nullList.isEmpty());

    webService.setStatusVariables("VAR1");
    List<String> singleList = webService.getStatusVariablesList(variables);
    assertEquals(1, singleList.size());
    assertEquals("VAR1", singleList.get(0));

    webService.setStatusVariables("VAR1,VAR2,VAR3");
    List<String> multipleList = webService.getStatusVariablesList(variables);
    assertEquals(3, multipleList.size());
    assertEquals("VAR1", multipleList.get(0));
    assertEquals("VAR2", multipleList.get(1));
    assertEquals("VAR3", multipleList.get(2));
  }

  @Test
  public void testGetStatusVariablesListWithVariableResolution() {
    AsyncWebService webService = new AsyncWebService();
    Variables variables = new Variables();

    variables.setVariable("STATUS_VARS", "RESOLVED_VAR1,RESOLVED_VAR2");
    webService.setStatusVariables("${STATUS_VARS}");

    List<String> resolvedList = webService.getStatusVariablesList(variables);
    assertEquals(2, resolvedList.size());
    assertEquals("RESOLVED_VAR1", resolvedList.get(0));
    assertEquals("RESOLVED_VAR2", resolvedList.get(1));
  }
}
