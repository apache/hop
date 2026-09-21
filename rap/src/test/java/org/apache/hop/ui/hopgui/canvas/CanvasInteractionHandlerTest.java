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

package org.apache.hop.ui.hopgui.canvas;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.ui.hopgui.shared.IWebCanvasGraph;
import org.eclipse.rap.json.JsonObject;
import org.junit.jupiter.api.Test;

class CanvasInteractionHandlerTest {

  /** Records what the client hover notifications ask of the graph. */
  private static class RecordingGraph implements IWebCanvasGraph {
    final List<String> calls = new ArrayList<>();

    @Override
    public void replaceAreaOwners(List<AreaOwner> owners) {}

    @Override
    public void handleWebCanvasHover(int graphX, int graphY, int screenX, int screenY) {
      calls.add("hover " + graphX + "," + graphY + " @" + screenX + "," + screenY);
    }

    @Override
    public void handleWebCanvasHoverEnd() {
      calls.add("end");
    }
  }

  @Test
  void hoverCoordinatesReachTheGraphAndLeaveHidesTheTooltip() {
    CanvasGraphRegistry registry = new CanvasGraphRegistry();
    RecordingGraph graph = new RecordingGraph();
    registry.register("c1", null, graph);

    CanvasInteractionHandler.handleHover(
        registry,
        new JsonObject()
            .add("canvasId", "c1")
            .add("graphX", 10)
            .add("graphY", 20)
            .add("screenX", 110)
            .add("screenY", 120));
    // The pointer moved off the transform: the tooltip used to stay behind (no leave message).
    CanvasInteractionHandler.handleHover(
        registry, new JsonObject().add("canvasId", "c1").add("leave", true));
    // Unknown canvas and missing canvas id are ignored
    CanvasInteractionHandler.handleHover(
        registry, new JsonObject().add("canvasId", "nope").add("leave", true));
    CanvasInteractionHandler.handleHover(registry, new JsonObject().add("leave", true));

    assertEquals(List.of("hover 10,20 @110,120", "end"), graph.calls);
  }
}
