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

package org.apache.hop.core.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class HopServerEndpointPermissionMapperTest {

  @Test
  void readEndpointsRequireFileView() {
    for (String path :
        new String[] {
          "/hop/status",
          "/hop/pipelineStatus",
          "/hop/workflowStatus",
          "/hop/pipelineImage",
          "/hop/workflowImage",
          "/hop/getExecInfo",
          "/hop/asyncStatus",
          "/hop/sniffTransform"
        }) {
      assertEquals(
          Optional.of(Permission.FILE_VIEW),
          HopServerEndpointPermissionMapper.requiredPermission(path),
          path);
    }
  }

  @Test
  void deployEndpointsRequireFileSave() {
    for (String path :
        new String[] {
          "/hop/addPipeline",
          "/hop/addWorkflow",
          "/hop/registerPipeline",
          "/hop/registerWorkflow",
          "/hop/addExport",
          "/hop/registerPackage"
        }) {
      assertEquals(
          Optional.of(Permission.FILE_SAVE),
          HopServerEndpointPermissionMapper.requiredPermission(path),
          path);
    }
  }

  @Test
  void executionInfoStoreRequiresMetadataWrite() {
    assertEquals(
        Optional.of(Permission.METADATA_WRITE),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/registerExecInfo"));
    assertEquals(
        Optional.of(Permission.METADATA_WRITE),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/deleteExecInfo"));
  }

  @Test
  void executeEndpointsRequireRunExecute() {
    for (String path :
        new String[] {
          "/hop/prepareExec",
          "/hop/startExec",
          "/hop/execPipeline",
          "/hop/execWorkflow",
          "/hop/startPipeline",
          "/hop/startWorkflow",
          "/hop/asyncRun",
          "/hop/webService"
        }) {
      assertEquals(
          Optional.of(Permission.RUN_EXECUTE),
          HopServerEndpointPermissionMapper.requiredPermission(path),
          path);
    }
  }

  @Test
  void controlEndpointsRequireRunStop() {
    for (String path :
        new String[] {"/hop/stopPipeline", "/hop/stopWorkflow", "/hop/pausePipeline"}) {
      assertEquals(
          Optional.of(Permission.RUN_STOP),
          HopServerEndpointPermissionMapper.requiredPermission(path),
          path);
    }
  }

  @Test
  void removeEndpointsRequireFileDelete() {
    assertEquals(
        Optional.of(Permission.FILE_DELETE),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/removePipeline"));
    assertEquals(
        Optional.of(Permission.FILE_DELETE),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/removeWorkflow"));
  }

  @Test
  void readOnlyRoleCanReadButNotMutateOrRun() {
    // Guards the core promise of the fix: the READ_ONLY role passes the read endpoints and is
    // refused deploy / run / stop / delete.
    HopSecurityContext readonly =
        HopSecurityContext.forUser("viewer", java.util.Set.of(HopRole.READ_ONLY));

    assertTrue(
        readonly.allows(
            HopServerEndpointPermissionMapper.requiredPermission("/hop/status").orElseThrow()));
    assertTrue(
        readonly.allows(
            HopServerEndpointPermissionMapper.requiredPermission("/hop/pipelineStatus")
                .orElseThrow()));

    assertFalse(
        readonly.allows(
            HopServerEndpointPermissionMapper.requiredPermission("/hop/addWorkflow")
                .orElseThrow()));
    assertFalse(
        readonly.allows(
            HopServerEndpointPermissionMapper.requiredPermission("/hop/startWorkflow")
                .orElseThrow()));
    assertFalse(
        readonly.allows(
            HopServerEndpointPermissionMapper.requiredPermission("/hop/removePipeline")
                .orElseThrow()));
  }

  @Test
  void longestPrefixMatchHandlesTrailingSegments() {
    assertEquals(
        Optional.of(Permission.FILE_VIEW),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/pipelineStatus/my-pipe/1234"));
    assertEquals(
        Optional.of(Permission.RUN_EXECUTE),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/startPipeline/"));
  }

  @Test
  void jsessionidAndQueryAreStripped() {
    assertEquals(
        Optional.of(Permission.RUN_EXECUTE),
        HopServerEndpointPermissionMapper.requiredPermission(
            "/hop/startWorkflow;jsessionid=ABC123"));
    assertEquals(
        Optional.of(Permission.RUN_EXECUTE),
        HopServerEndpointPermissionMapper.requiredPermission("/hop/startWorkflow?name=x&id=y"));
  }

  @Test
  void unknownEndpointsAreNotKnownAndHaveNoPermission() {
    assertFalse(HopServerEndpointPermissionMapper.isKnownEndpoint("/hop/somethingNew"));
    assertFalse(HopServerEndpointPermissionMapper.isKnownEndpoint("/hop"));
    assertFalse(HopServerEndpointPermissionMapper.isKnownEndpoint("/"));
    assertTrue(HopServerEndpointPermissionMapper.requiredPermission("/hop/nope").isEmpty());
  }

  @Test
  void nullAndBlankAreSafe() {
    assertTrue(HopServerEndpointPermissionMapper.requiredPermission(null).isEmpty());
    assertTrue(HopServerEndpointPermissionMapper.requiredPermission("  ").isEmpty());
    assertFalse(HopServerEndpointPermissionMapper.isKnownEndpoint(null));
  }
}
