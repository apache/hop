/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.ftpput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** ActionFtpPut test */
class ActionFtpPutTests {
  private ActionFtpPut action;

  @BeforeEach
  void setUp() throws HopException {
    action = new ActionFtpPut("Test Put a file with FTP");
    action.setUserName("user");
    action.setPassword("password");
    action.setServerName("127.0.0.1");
    action.setName("Test name");
    action.setRemoteDirectory("/home/user");
    action.setLocalDirectory("/tmp");

    HopEnvironment.init();
  }

  @Test
  void testEmptyActionFtpPut() {
    assertEquals("", action.getDescription());

    ActionFtpPut ftpPut = new ActionFtpPut();
    assertTrue(ftpPut.getDescription().isBlank());
  }

  @Test
  void testClone() {
    Object cloned = action.clone();
    assertNotSame(cloned, action);
  }

  @Test
  void testIsEvaluation() {
    assertTrue(action.isEvaluation());
  }

  @Test
  void testGetResourceDependencies() {
    IVariables variables = mock(IVariables.class);
    WorkflowMeta meta = mock(WorkflowMeta.class);

    // 127.0.0.1 server
    List<ResourceReference> references = action.getResourceDependencies(variables, meta);
    assertNotNull(references);
    assertEquals(1, references.size());

    // null server
    action.setServerName(null);
    references = action.getResourceDependencies(variables, meta);
    assertNotNull(references);
    assertTrue(references.isEmpty());
  }

  @Test
  void testCheck() {
    List<ICheckResult> remarks = new ArrayList<>();
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    IVariables variables = mock(IVariables.class);
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);

    // server is null
    action.setServerName(Const.EMPTY_STRING);
    action.check(remarks, workflowMeta, variables, provider);

    boolean hasError =
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR);
    assertTrue(hasError);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testExecuteSuccess() throws Exception {
    Path tempDir = Files.createTempDirectory("ftpTest");
    Path tempFile = Files.createTempFile(tempDir, "file_", ".txt");

    try (MockedStatic<HopVfs> ignored = mockStatic(HopVfs.class)) {
      action = spy(new ActionFtpPut("Test FTP Action"));
      action.setServerName("127.0.0.1");
      action.setUserName("user");
      action.setPassword("pass");
      action.setRemoteDirectory("/remote");
      IWorkflowEngine<WorkflowMeta> workflowEngine = mock(IWorkflowEngine.class);
      when(workflowEngine.isStopped()).thenReturn(false);
      when(workflowEngine.getLogLevel()).thenReturn(LogLevel.BASIC);
      when(workflowEngine.getContainerId()).thenReturn("test-container");
      action.setParentWorkflow(workflowEngine);

      action.setLocalDirectory(tempDir.toString());

      FTPClient mockFtp = mock(FTPClient.class);
      when(mockFtp.isConnected()).thenReturn(true);
      when(mockFtp.storeFile(anyString(), any(InputStream.class))).thenReturn(true);
      when(mockFtp.getReplyCode()).thenReturn(230);
      when(mockFtp.getReplyString()).thenReturn("OK");

      doReturn(mockFtp).when(action).createAndSetUpFtpClient();

      when(HopVfs.getInputStream(anyString()))
          .thenAnswer(invocation -> new ByteArrayInputStream("test content".getBytes()));

      Result result = new Result();
      result = action.execute(result, 0);

      assertTrue(result.isResult(), "execute success");
      assertEquals(0, result.getNrErrors());
    } finally {
      Files.deleteIfExists(tempFile);
      Files.deleteIfExists(tempDir);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testExecuteFailure() throws Exception {
    Path tempDir = Files.createTempDirectory("ftpTest");
    Path tempFile = Files.createTempFile(tempDir, "file_", ".txt");

    try (MockedStatic<HopVfs> ignored = mockStatic(HopVfs.class)) {
      action = spy(new ActionFtpPut("Test FTP Action"));
      action.setServerName("127.0.0.1");
      action.setUserName("user");
      action.setPassword("pass");
      action.setRemoteDirectory("/remote");
      IWorkflowEngine<WorkflowMeta> workflowEngine = mock(IWorkflowEngine.class);
      when(workflowEngine.isStopped()).thenReturn(false);
      when(workflowEngine.getLogLevel()).thenReturn(LogLevel.BASIC);
      when(workflowEngine.getContainerId()).thenReturn("test-container");
      action.setParentWorkflow(workflowEngine);

      action.setLocalDirectory(tempDir.toString());

      FTPClient mockFtp = mock(FTPClient.class);
      when(mockFtp.isConnected()).thenReturn(true);
      when(mockFtp.storeFile(anyString(), any(InputStream.class))).thenReturn(true);
      when(mockFtp.getReplyCode()).thenReturn(530);
      when(mockFtp.getReplyString()).thenReturn("530 Login incorrect");

      doReturn(mockFtp).when(action).createAndSetUpFtpClient();

      when(HopVfs.getInputStream(anyString()))
          .thenAnswer(invocation -> new ByteArrayInputStream("test content".getBytes()));

      Result result = new Result();
      result = action.execute(result, 0);

      assertFalse(result.isResult(), "530 Login incorrect");
      assertEquals(1, result.getNrErrors());
    } finally {
      Files.deleteIfExists(tempFile);
      Files.deleteIfExists(tempDir);
    }
  }
}
