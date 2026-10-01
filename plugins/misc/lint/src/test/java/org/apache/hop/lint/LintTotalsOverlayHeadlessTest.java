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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowPainter;
import org.junit.jupiter.api.Test;

/**
 * The lint totals overlay is a canvas feature, but the painter extension points drawing it also run
 * on Hop Server, which renders pipeline and workflow SVG images without a user interface. Asking
 * SWT for the native zoom factor there throws an Error rather than an Exception, which escaped
 * every catch on the way out and left /hop/pipelineImage answering an HTTP 500 (issue #8239).
 */
public class LintTotalsOverlayHeadlessTest {

  @Test
  public void overlayZoomFactorWithoutHopGuiReturnsNull() {
    assertNull(HopGui.peekInstance(), "precondition: no GUI in this test JVM");

    assertNull(LintCanvasOverlayHelper.overlayZoomFactor());

    assertNull(HopGui.peekInstance(), "asking for the zoom factor must not build a HopGui");
  }

  @Test
  public void pipelineTotalsOverlayIsSkippedWithoutUserInterface() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename("test.hpl");

    IGc gc = mock(IGc.class);
    PipelinePainter painter = mock(PipelinePainter.class);
    when(painter.getPipelineMeta()).thenReturn(pipelineMeta);
    when(painter.getGc()).thenReturn(gc);
    when(painter.getOffset()).thenReturn(new DPoint(0, 0));

    new PipelineLintTotalsPainterExtension()
        .callExtensionPoint(mock(ILogChannel.class), mock(IVariables.class), painter);

    verifyNoInteractions(gc);
  }

  @Test
  public void workflowTotalsOverlayIsSkippedWithoutUserInterface() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setFilename("test.hwf");

    IGc gc = mock(IGc.class);
    WorkflowPainter painter = mock(WorkflowPainter.class);
    when(painter.getWorkflowMeta()).thenReturn(workflowMeta);
    when(painter.getGc()).thenReturn(gc);
    when(painter.getOffset()).thenReturn(new DPoint(0, 0));

    new WorkflowLintTotalsPainterExtension()
        .callExtensionPoint(mock(ILogChannel.class), mock(IVariables.class), painter);

    verifyNoInteractions(gc);
  }
}
