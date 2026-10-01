/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
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

import java.util.Optional;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopServerEndpointPermissionMapper;
import org.apache.hop.core.security.Permission;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HopServerPluginPermissionsTest {

  @BeforeAll
  static void initLog() {
    HopLogStore.init();
  }

  @AfterEach
  void clearOverlay() throws Exception {
    HopServerEndpointPermissionMapper.unregister("/hop/sourceModelData");
  }

  @Test
  void jdbcTokenServletDeclaresFileView() {
    JdbcTokenServlet servlet = new JdbcTokenServlet();
    assertEquals("file.view", servlet.getRequiredPermissionId());
    assertEquals("/hop/jdbcToken", servlet.getContextPath());
  }

  @Test
  void registerHonoursRequiredPermissionFromThePlugin() {
    IHopServerPlugin plugin =
        new IHopServerPlugin() {
          @Override
          public void setup(PipelineMap pipelineMap, WorkflowMap workflowMap) {}

          @Override
          public void doGet(
              jakarta.servlet.http.HttpServletRequest request,
              jakarta.servlet.http.HttpServletResponse response) {}

          @Override
          public String getContextPath() {
            return "/hop/sourceModelData";
          }

          @Override
          public void setJettyMode(boolean jettyMode) {}

          @Override
          public boolean isJettyMode() {
            return false;
          }

          @Override
          public String getRequiredPermissionId() {
            return "run.execute";
          }

          @Override
          public String getService() {
            return getContextPath();
          }
        };

    HopServerPluginPermissions.register(plugin, new LogChannel("test"));
    assertEquals(
        Optional.of(Permission.RUN_EXECUTE),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/sourceModelData"));
    HopServerPluginPermissions.unregister(plugin);
    assertTrue(
        HopServerEndpointPermissionMapper.requiredPermission("/hop/sourceModelData").isEmpty());
  }
}
