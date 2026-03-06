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

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.owasp.encoder.Encode;

public class GetWorkflowStatusServletTest {
  private WorkflowMap mockWorkflowMap;

  private GetWorkflowStatusServlet getWorkflowStatusServlet;

  @Before
  public void setup() {
    mockWorkflowMap = mock(WorkflowMap.class);
    getWorkflowStatusServlet = new GetWorkflowStatusServlet(mockWorkflowMap);
  }

  @Test
  public void testGetJobStatusServletEscapesHtmlWhenPipelineNotFound()
      throws ServletException, IOException {
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);

    StringWriter out = new StringWriter();
    PrintWriter printWriter = new PrintWriter(out);

    Mockito.spy(Encode.class);
    when(mockHttpServletRequest.getContextPath()).thenReturn(GetWorkflowStatusServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter(anyString()))
        .thenReturn(ServletTestUtils.BAD_STRING_TO_TEST);
    when(mockHttpServletResponse.getWriter()).thenReturn(printWriter);

    getWorkflowStatusServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);

    assertFalse(ServletTestUtils.hasBadText(ServletTestUtils.getInsideOfTag("H1", out.toString())));
  }

  @Test
  public void testGetJobStatusServletEscapesHtmlWhenPipelineFound()
      throws ServletException, IOException {
    HopLogStore.init();
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);
    IWorkflowEngine<WorkflowMeta> mockWorkflow = Mockito.mock(Workflow.class);
    WorkflowMeta mockWorkflowMeta = mock(WorkflowMeta.class);
    ILogChannel mockLogChannelInterface = mock(ILogChannel.class);
    StringWriter out = new StringWriter();
    PrintWriter printWriter = new PrintWriter(out);

    Mockito.spy(Encode.class);
    when(mockHttpServletRequest.getContextPath()).thenReturn(GetWorkflowStatusServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter(anyString()))
        .thenReturn(ServletTestUtils.BAD_STRING_TO_TEST);
    when(mockHttpServletResponse.getWriter()).thenReturn(printWriter);
    when(mockWorkflowMap.getWorkflow(any(HopServerObjectEntry.class))).thenReturn(mockWorkflow);
    Mockito.when(mockWorkflow.getWorkflowName()).thenReturn(ServletTestUtils.BAD_STRING_TO_TEST);
    Mockito.when(mockWorkflow.getLogChannel()).thenReturn(mockLogChannelInterface);
    Mockito.when(mockWorkflow.getWorkflowMeta()).thenReturn(mockWorkflowMeta);
    Mockito.when(mockWorkflowMeta.getMaximum()).thenReturn(new Point(10, 10));

    getWorkflowStatusServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);
    assertFalse(out.toString().contains(ServletTestUtils.BAD_STRING_TO_TEST));
  }

  @Test
  public void testGetJobStatus() throws ServletException, IOException {
    HopLogStore.init();
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);
    IWorkflowEngine<WorkflowMeta> mockWorkflow = Mockito.mock(Workflow.class);
    WorkflowMeta mockWorkflowMeta = mock(WorkflowMeta.class);
    ILogChannel mockLogChannelInterface = mock(ILogChannel.class);
    ServletOutputStream outMock = mock(ServletOutputStream.class);

    String id = "123";
    String logId = "logId";
    String useXml = "Y";

    when(mockHttpServletRequest.getContextPath()).thenReturn(GetWorkflowStatusServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter("id")).thenReturn(id);
    when(mockHttpServletRequest.getParameter("xml")).thenReturn(useXml);
    when(mockHttpServletResponse.getOutputStream()).thenReturn(outMock);
    when(mockWorkflowMap.findWorkflow(id)).thenReturn(mockWorkflow);
    Mockito.when(mockWorkflow.getWorkflowName()).thenReturn(ServletTestUtils.BAD_STRING_TO_TEST);
    Mockito.when(mockWorkflow.getLogChannel()).thenReturn(mockLogChannelInterface);
    Mockito.when(mockWorkflow.getWorkflowMeta()).thenReturn(mockWorkflowMeta);
    Mockito.when(mockWorkflow.isFinished()).thenReturn(true);
    Mockito.when(mockWorkflow.getLogChannelId()).thenReturn(logId);
    Mockito.when(mockWorkflowMeta.getMaximum()).thenReturn(new Point(10, 10));
    when(mockWorkflow.getStatusDescription()).thenReturn("Finished");

    getWorkflowStatusServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);

    getWorkflowStatusServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);

    verify(mockWorkflow, times(2)).getLogChannel();
  }
}
