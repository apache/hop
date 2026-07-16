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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
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
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.owasp.encoder.Encode;

class GetWorkflowStatusServletTest {
  private WorkflowMap mockWorkflowMap;

  private GetWorkflowStatusServlet getWorkflowStatusServlet;

  @BeforeEach
  void setup() {
    mockWorkflowMap = mock(WorkflowMap.class);
    getWorkflowStatusServlet = new GetWorkflowStatusServlet(mockWorkflowMap);
  }

  @Test
  void testGetJobStatusServletEscapesHtmlWhenPipelineNotFound()
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
  void testGetJobStatusServletEscapesHtmlWhenPipelineFound() throws ServletException, IOException {
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

  /**
   * The workflow status has to report when the workflow started and ended, the way the pipeline
   * status does. See issue #4052.
   */
  @Test
  void statusReportsExecutionStartAndEndDate() throws ServletException, IOException {
    Date startDate = new Date(1700000000000L);
    Date endDate = new Date(1700000012345L);

    String xml = getStatusOutput(startDate, endDate, "Y", null);

    assertTrue(
        xml.contains("<execution_start_date>" + XmlHandler.date2string(startDate)),
        "The status should report the start date, was: " + xml);
    assertTrue(
        xml.contains("<execution_end_date>" + XmlHandler.date2string(endDate)),
        "The status should report the end date, was: " + xml);
  }

  /** The same dates are reported in JSON, which is what the REST API of the server serves. */
  @Test
  void jsonStatusReportsExecutionStartAndEndDate() throws ServletException, IOException {
    Date startDate = new Date(1700000000000L);
    Date endDate = new Date(1700000012345L);

    String json = getStatusOutput(startDate, endDate, null, "Y");

    assertTrue(json.contains("\"executionStartDate\""), "The JSON should hold the start date");
    assertTrue(json.contains("\"executionEndDate\""), "The JSON should hold the end date");
    assertFalse(
        json.contains("\"executionStartDate\" : null"),
        "The start date should not be null: " + json);
    assertFalse(
        json.contains("\"executionEndDate\" : null"), "The end date should not be null: " + json);
  }

  /** Runs the servlet for a finished workflow and returns what it wrote to the response. */
  private String getStatusOutput(Date startDate, Date endDate, String useXml, String useJson)
      throws ServletException, IOException {
    HopLogStore.init();
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockHttpServletResponse = mock(HttpServletResponse.class);
    IWorkflowEngine<WorkflowMeta> mockWorkflow = Mockito.mock(Workflow.class);
    WorkflowMeta mockWorkflowMeta = mock(WorkflowMeta.class);
    ILogChannel mockLogChannelInterface = mock(ILogChannel.class);
    ServletOutputStream outMock = mock(ServletOutputStream.class);

    String id = "123";

    when(mockHttpServletRequest.getContextPath()).thenReturn(GetWorkflowStatusServlet.CONTEXT_PATH);
    when(mockHttpServletRequest.getParameter("id")).thenReturn(id);
    when(mockHttpServletRequest.getParameter("xml")).thenReturn(useXml);
    when(mockHttpServletRequest.getParameter("json")).thenReturn(useJson);
    when(mockHttpServletResponse.getOutputStream()).thenReturn(outMock);
    when(mockWorkflowMap.findWorkflow(id)).thenReturn(mockWorkflow);
    Mockito.when(mockWorkflow.getWorkflowName()).thenReturn("a-workflow");
    Mockito.when(mockWorkflow.getLogChannel()).thenReturn(mockLogChannelInterface);
    Mockito.when(mockWorkflow.getWorkflowMeta()).thenReturn(mockWorkflowMeta);
    Mockito.when(mockWorkflow.isFinished()).thenReturn(true);
    Mockito.when(mockWorkflow.getLogChannelId()).thenReturn("logId");
    Mockito.when(mockWorkflow.getExecutionStartDate()).thenReturn(startDate);
    Mockito.when(mockWorkflow.getExecutionEndDate()).thenReturn(endDate);
    Mockito.when(mockWorkflowMeta.getMaximum()).thenReturn(new Point(10, 10));
    when(mockWorkflow.getStatusDescription()).thenReturn("Finished");

    getWorkflowStatusServlet.doGet(mockHttpServletRequest, mockHttpServletResponse);

    ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
    verify(outMock, atLeastOnce()).write(captor.capture());
    StringBuilder written = new StringBuilder();
    for (byte[] data : captor.getAllValues()) {
      written.append(new String(data, StandardCharsets.UTF_8));
    }
    return written.toString();
  }

  @Test
  void testGetJobStatus() throws ServletException, IOException {
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
