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

package org.apache.hop.web.it;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.web.it.pages.PipelineGraphPage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Working on the canvas itself: moving things, undoing, zooming.
 *
 * <p>None of this is shared with the fat client. The graph is drawn into a {@code <canvas>} whose
 * mouse handling, drag tracking and zoom are Hop Web's own, and every one of them has been broken
 * at least once - dragging in issue #7227, zoom in #6442, and a canvas that reacted to a click on
 * the preview icon by going into drag mode in #6285.
 */
@DisplayName("The canvas")
class HopWebCanvasTest extends HopWebTestBase {

  private static final String TRANSFORM = "Generate rows";

  /** Half a transform's width; a move has to be bigger than this to be visible at all. */
  private static final int TOLERANCE = 25;

  @Test
  @DisplayName("a transform can be dragged somewhere else")
  void dragsATransform() {
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(TRANSFORM);
    int[] before = graph.transformOffset(TRANSFORM);

    graph.dragTransform(TRANSFORM, 150, 90);

    wait.until(d -> Math.abs(graph.transformOffset(TRANSFORM)[0] - before[0]) > TOLERANCE);
    int[] after = graph.transformOffset(TRANSFORM);
    assertTrue(
        Math.abs(after[0] - before[0] - 150) < TOLERANCE
            && Math.abs(after[1] - before[1] - 90) < TOLERANCE,
        () ->
            "expected the transform to move by about 150,90 but it went from "
                + before[0]
                + ","
                + before[1]
                + " to "
                + after[0]
                + ","
                + after[1]);
  }

  @Test
  @DisplayName("undo takes a transform away again, redo puts it back")
  void undoesAndRedoes() {
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(TRANSFORM);

    graph.undo(hopGui);

    wait.until(d -> !graph.contains(TRANSFORM));
    assertFalse(graph.contains(TRANSFORM), () -> "undo left " + graph.labels());

    graph.redo(hopGui);

    wait.until(d -> graph.contains(TRANSFORM));
    assertTrue(graph.contains(TRANSFORM), () -> "redo left " + graph.labels());
  }

  @Test
  @DisplayName("zooming in and out changes how big the graph is drawn")
  void zooms() {
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(TRANSFORM);
    int normal = graph.transformIconSize(TRANSFORM);

    hopGui.clickWidget(PipelineGraphPage.ZOOM_IN);

    wait.until(d -> graph.transformIconSize(TRANSFORM) > normal);
    int zoomedIn = graph.transformIconSize(TRANSFORM);

    hopGui.clickWidget(PipelineGraphPage.ZOOM_OUT);

    wait.until(d -> graph.transformIconSize(TRANSFORM) < zoomedIn);
    assertTrue(
        graph.transformIconSize(TRANSFORM) <= normal,
        () -> "zooming back out left the icon at " + graph.transformIconSize(TRANSFORM));
  }
}
