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

package org.apache.hop.annotation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Event;
import org.junit.jupiter.api.Test;

class AnnotationClicksTest {

  private static final String PRESSED = "annotation-callout-pressed";

  @Test
  void pressOnOneGraphDoesNotSwallowAnotherGraphsMouseUp() {
    Graph pipelineA = new Graph(true);
    Graph pipelineB = new Graph(true);
    Graph workflowB = new Graph(false);

    AnnotationClicks.pipelineMouseDown(pipelineA.pipelineDown(callout(), 1));
    assertEquals(Boolean.TRUE, pipelineA.data.get(PRESSED));
    assertNull(pipelineB.data.get(PRESSED));

    HopGuiPipelineGraphExtension otherPipeline = pipelineB.pipelineUp(notCallout());
    AnnotationClicks.pipelineMouseUp(otherPipeline);
    assertFalse(otherPipeline.isPreventingDefault());
    assertEquals(Boolean.TRUE, pipelineA.data.get(PRESSED));

    HopGuiWorkflowGraphExtension otherWorkflow = workflowB.workflowUp(notCallout());
    AnnotationClicks.workflowMouseUp(otherWorkflow);
    assertFalse(otherWorkflow.isPreventingDefault());
    assertEquals(Boolean.TRUE, pipelineA.data.get(PRESSED));
    assertNull(workflowB.data.get(PRESSED));

    // Releasing on a callout is still claimed, and only that graph's flag is cleared.
    HopGuiPipelineGraphExtension onCallout = pipelineB.pipelineUp(callout());
    AnnotationClicks.pipelineMouseUp(onCallout);
    assertTrue(onCallout.isPreventingDefault());
    assertEquals(Boolean.TRUE, pipelineA.data.get(PRESSED));
    assertNull(pipelineB.data.get(PRESSED));

    HopGuiPipelineGraphExtension release = pipelineA.pipelineUp(notCallout());
    AnnotationClicks.pipelineMouseUp(release);
    assertTrue(release.isPreventingDefault());
    assertNull(pipelineA.data.get(PRESSED));

    HopGuiPipelineGraphExtension again = pipelineA.pipelineUp(notCallout());
    AnnotationClicks.pipelineMouseUp(again);
    assertFalse(again.isPreventingDefault());

    Graph workflowA = new Graph(false);
    AnnotationClicks.workflowMouseDown(workflowA.workflowDown(callout(), 1));
    assertEquals(Boolean.TRUE, workflowA.data.get(PRESSED));
    HopGuiPipelineGraphExtension pipelineRelease = pipelineB.pipelineUp(notCallout());
    AnnotationClicks.pipelineMouseUp(pipelineRelease);
    assertFalse(pipelineRelease.isPreventingDefault());
    assertEquals(Boolean.TRUE, workflowA.data.get(PRESSED));

    HopGuiWorkflowGraphExtension workflowRelease = workflowA.workflowUp(notCallout());
    AnnotationClicks.workflowMouseUp(workflowRelease);
    assertTrue(workflowRelease.isPreventingDefault());
    assertNull(workflowA.data.get(PRESSED));
  }

  @Test
  void mouseDownOffTheCalloutClearsOnlyThatGraph() {
    Graph pipelineA = new Graph(true);
    Graph pipelineB = new Graph(true);
    AnnotationClicks.pipelineMouseDown(pipelineA.pipelineDown(callout(), 1));
    AnnotationClicks.pipelineMouseDown(pipelineB.pipelineDown(notCallout(), 1));
    assertEquals(Boolean.TRUE, pipelineA.data.get(PRESSED));
    assertNull(pipelineB.data.get(PRESSED));

    AnnotationClicks.pipelineMouseDown(pipelineA.pipelineDown(notCallout(), 1));
    assertNull(pipelineA.data.get(PRESSED));
    HopGuiPipelineGraphExtension up = pipelineA.pipelineUp(notCallout());
    AnnotationClicks.pipelineMouseUp(up);
    assertFalse(up.isPreventingDefault());
  }

  @Test
  void nullGraphAndDisposedCanvasDoNotThrow() {
    assertDoesNotThrow(() -> AnnotationClicks.pipelineMouseDown(null));
    assertDoesNotThrow(() -> AnnotationClicks.pipelineMouseUp(null));
    assertDoesNotThrow(() -> AnnotationClicks.workflowMouseDown(null));
    assertDoesNotThrow(() -> AnnotationClicks.workflowMouseUp(null));

    HopGuiPipelineGraph graph = mock(HopGuiPipelineGraph.class);
    when(graph.getCanvas()).thenReturn(null);
    HopGuiPipelineGraphExtension down =
        new HopGuiPipelineGraphExtension(graph, mouse(1), new Point(0, 0), callout());
    AnnotationClicks.pipelineMouseDown(down);
    assertTrue(down.isPreventingDefault());
    HopGuiPipelineGraphExtension up =
        new HopGuiPipelineGraphExtension(graph, mouse(1), new Point(0, 0), notCallout());
    AnnotationClicks.pipelineMouseUp(up);
    assertFalse(up.isPreventingDefault());

    Canvas disposed = mock(Canvas.class);
    when(disposed.isDisposed()).thenReturn(true);
    when(graph.getCanvas()).thenReturn(disposed);
    HopGuiPipelineGraphExtension disposedDown =
        new HopGuiPipelineGraphExtension(graph, mouse(1), new Point(0, 0), callout());
    AnnotationClicks.pipelineMouseDown(disposedDown);
    assertTrue(disposedDown.isPreventingDefault());
    verify(disposed, never()).setData(anyString(), nullable(Object.class));
  }

  private static AreaOwner callout() {
    return new AreaOwner(
        AreaOwner.AreaType.CUSTOM,
        0,
        0,
        10,
        10,
        new DPoint(0, 0),
        new AnnotationHit("anchor"),
        "anchor");
  }

  private static AreaOwner notCallout() {
    return new AreaOwner(AreaOwner.AreaType.NOTE, 0, 0, 10, 10, new DPoint(0, 0), "note", "note");
  }

  private static MouseEvent mouse(int button) {
    Event event = new Event();
    event.button = button;
    event.widget = mock(Canvas.class);
    return new MouseEvent(event);
  }

  /** One graph and the data stored on its canvas. */
  private static final class Graph {
    private final Map<String, Object> data = new HashMap<>();
    private final HopGuiPipelineGraph pipeline;
    private final HopGuiWorkflowGraph workflow;

    private Graph(boolean pipelineGraph) {
      Canvas canvas = mock(Canvas.class);
      when(canvas.isDisposed()).thenReturn(false);
      when(canvas.getData(anyString()))
          .thenAnswer(invocation -> data.get(invocation.getArgument(0, String.class)));
      doAnswer(
              invocation -> {
                data.put(invocation.getArgument(0, String.class), invocation.getArgument(1));
                return null;
              })
          .when(canvas)
          .setData(anyString(), nullable(Object.class));
      if (pipelineGraph) {
        pipeline = mock(HopGuiPipelineGraph.class);
        when(pipeline.getCanvas()).thenReturn(canvas);
        workflow = null;
      } else {
        pipeline = null;
        workflow = mock(HopGuiWorkflowGraph.class);
        when(workflow.getCanvas()).thenReturn(canvas);
      }
    }

    private HopGuiPipelineGraphExtension pipelineDown(AreaOwner area, int button) {
      return new HopGuiPipelineGraphExtension(pipeline, mouse(button), new Point(0, 0), area);
    }

    private HopGuiPipelineGraphExtension pipelineUp(AreaOwner area) {
      return pipelineDown(area, 1);
    }

    private HopGuiWorkflowGraphExtension workflowDown(AreaOwner area, int button) {
      return new HopGuiWorkflowGraphExtension(workflow, mouse(button), new Point(0, 0), area);
    }

    private HopGuiWorkflowGraphExtension workflowUp(AreaOwner area) {
      return workflowDown(area, 1);
    }
  }
}
