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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Light coverage for servlet identity methods (context path, service description) on plugins that
 * are expensive to exercise end-to-end.
 */
class HopServerPluginServletSmokeTest {

  private static void assertPluginPaths(IHopServerPlugin servlet, String expectedContextPath) {
    assertEquals(expectedContextPath, servlet.getContextPath());
    assertTrue(servlet.getService().startsWith(expectedContextPath));
  }

  @Test
  void registerWorkflowServletPaths() {
    assertPluginPaths(new RegisterWorkflowServlet(), RegisterWorkflowServlet.CONTEXT_PATH);
  }

  @Test
  void deleteExecutionInfoServletPaths() {
    assertPluginPaths(new DeleteExecutionInfoServlet(), DeleteExecutionInfoServlet.CONTEXT_PATH);
  }

  @Test
  void execPipelineServletPaths() {
    assertPluginPaths(new ExecPipelineServlet(), ExecPipelineServlet.CONTEXT_PATH);
  }

  @Test
  void execWorkflowServletPaths() {
    assertPluginPaths(new ExecWorkflowServlet(), ExecWorkflowServlet.CONTEXT_PATH);
  }
}
