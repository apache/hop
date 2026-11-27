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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StopPipelineServletTest {
  private PipelineMap pipelineMap;
  private StopPipelineServlet servlet;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    pipelineMap = mock(PipelineMap.class);
    servlet = new StopPipelineServlet(pipelineMap);
  }

  @Test
  void inputOnlyStopsTheInputTransformsOfALocalPipeline() throws Exception {
    Pipeline pipeline = mock(Pipeline.class);

    stop(pipeline, "Y");

    verify(pipeline).safeStop();
    verify(pipeline, never()).stopAll();
  }

  @Test
  void withoutInputOnlyTheWholePipelineStops() throws Exception {
    Pipeline pipeline = mock(Pipeline.class);

    stop(pipeline, null);

    verify(pipeline).stopAll();
    verify(pipeline, never()).safeStop();
  }

  @Test
  void inputOnlyStopsAnEngineWithoutASafeStopCompletely() throws Exception {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);

    stop(pipeline, "Y");

    verify(pipeline).stopAll();
  }

  private void stop(IPipelineEngine<PipelineMeta> pipeline, String inputOnly) throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getContextPath()).thenReturn(StopPipelineServlet.CONTEXT_PATH);
    when(request.getParameter("name")).thenReturn("pipeline");
    when(request.getParameter("id")).thenReturn("id");
    when(request.getParameter("xml")).thenReturn("Y");
    when(request.getParameter("inputOnly")).thenReturn(inputOnly);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    when(pipelineMap.getPipeline(any(HopServerObjectEntry.class))).thenReturn(pipeline);

    servlet.doGet(request, response);
  }
}
