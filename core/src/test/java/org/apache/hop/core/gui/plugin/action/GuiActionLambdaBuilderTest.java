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

package org.apache.hop.core.gui.plugin.action;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.gui.plugin.IGuiRefresher;
import org.junit.jupiter.api.Test;

class GuiActionLambdaBuilderTest {

  @Test
  void actionRunsOnTheRefresherWhenGetInstanceReturnsNull() throws Exception {
    ClickedGraph.active = null;
    ClickedGraph graph = new ClickedGraph();
    GuiAction action =
        new GuiAction(
            "mark",
            GuiActionType.Modify,
            "mark",
            "mark",
            null,
            ClickedGraph.class.getName(),
            "mark");

    GuiAction built =
        new GuiActionLambdaBuilder<Marker>().createLambda(action, new Marker(), graph);
    built.getActionLambda().executeAction(false, false);

    assertTrue(graph.marked, "the action runs on the graph that was clicked");
    assertTrue(graph.updated, "that graph is refreshed afterwards");
  }

  @Test
  void actionUsesGetInstanceWhenTheRefresherIsADifferentClass() throws Exception {
    Plugin.instance.marked = false;
    OtherRefresher refresher = new OtherRefresher();
    GuiAction action =
        new GuiAction(
            "mark", GuiActionType.Modify, "mark", "mark", null, Plugin.class.getName(), "mark");

    GuiAction built =
        new GuiActionLambdaBuilder<Marker>().createLambda(action, new Marker(), refresher);
    built.getActionLambda().executeAction(false, false);

    assertTrue(Plugin.instance.marked, "a plugin action still uses its singleton");
    assertTrue(refresher.updated);
  }

  @Test
  void missingPluginInstanceFailsWhenTheActionIsBuilt() {
    GuiAction action =
        new GuiAction(
            "mark",
            GuiActionType.Modify,
            "mark",
            "mark",
            null,
            MissingPlugin.class.getName(),
            "mark");

    HopRuntimeException exception =
        assertThrows(
            HopRuntimeException.class,
            () ->
                new GuiActionLambdaBuilder<Marker>()
                    .createLambda(action, new Marker(), new OtherRefresher()));
    assertTrue(exception.getMessage().contains(MissingPlugin.class.getName()));
  }

  public static final class Marker {}

  /** Stands in for a pipeline or workflow graph that is not the active tab. */
  public static final class ClickedGraph implements IGuiRefresher {
    static ClickedGraph active;
    boolean marked;
    boolean updated;

    public static ClickedGraph getInstance() {
      return active;
    }

    public void mark(Marker marker) {
      marked = marker != null;
    }

    @Override
    public void updateGui() {
      updated = true;
    }
  }

  /** {@code getInstance()} returns null and the refresher is a different class. */
  public static final class MissingPlugin {
    public static MissingPlugin getInstance() {
      return null;
    }

    public void mark(Marker marker) {}
  }

  public static final class Plugin {
    static final Plugin instance = new Plugin();
    boolean marked;

    public static Plugin getInstance() {
      return instance;
    }

    public void mark(Marker marker) {
      marked = marker != null;
    }
  }

  private static final class OtherRefresher implements IGuiRefresher {
    boolean updated;

    @Override
    public void updateGui() {
      updated = true;
    }
  }
}
