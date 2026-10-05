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

package org.apache.hop.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.layout.LayeredGraphLayout;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.Test;

public class WorkflowMetaLayoutTest {

  private ActionMeta action(String name) {
    ActionDummy dummy = new ActionDummy();
    dummy.setName(name);
    ActionMeta meta = new ActionMeta(dummy);
    meta.setLocation(0, 0);
    return meta;
  }

  private void hop(WorkflowMeta meta, ActionMeta from, ActionMeta to) {
    meta.addWorkflowHop(new WorkflowHopMeta(from, to));
  }

  private void assertOnGrid(Point p, int gridSize) {
    assertEquals(0, Math.floorMod(p.x, gridSize), "x not on grid: " + p.x);
    assertEquals(0, Math.floorMod(p.y, gridSize), "y not on grid: " + p.y);
  }

  @Test
  public void testLayoutNoOverlapAndLeftToRight() {
    WorkflowMeta meta = new WorkflowMeta();
    ActionMeta a = action("a");
    ActionMeta b = action("b");
    ActionMeta c = action("c");
    ActionMeta d = action("d"); // fan: a -> d, then d -> c (join into c)
    meta.addAction(a);
    meta.addAction(b);
    meta.addAction(c);
    meta.addAction(d);
    hop(meta, a, b);
    hop(meta, b, c);
    hop(meta, a, d);
    hop(meta, d, c);

    WorkflowMetaLayout.layout(meta);

    // (a) no two actions share the same (x,y)
    Set<String> coords = new HashSet<>();
    for (int i = 0; i < meta.nrActions(); i++) {
      Point p = meta.getAction(i).getLocation();
      assertTrue(coords.add(p.x + ":" + p.y), "Duplicate coordinate at " + p.x + "," + p.y);
    }

    // (b) every forward hop goes left-to-right
    for (int i = 0; i < meta.nrWorkflowHops(); i++) {
      WorkflowHopMeta h = meta.getWorkflowHop(i);
      Point from = h.getFromAction().getLocation();
      Point to = h.getToAction().getLocation();
      assertTrue(
          to.x > from.x,
          h.getFromAction().getName()
              + " -> "
              + h.getToAction().getName()
              + " not left-to-right ("
              + from.x
              + " >= "
              + to.x
              + ")");
    }
  }

  @Test
  public void testPositionsAlignToDefaultGrid() {
    WorkflowMeta meta = new WorkflowMeta();
    ActionMeta a = action("a");
    ActionMeta b = action("b");
    ActionMeta c = action("c");
    a.setLocation(101, 203); // deliberately off-grid origins
    b.setLocation(302, 51);
    c.setLocation(17, 19);
    meta.addAction(a);
    meta.addAction(b);
    meta.addAction(c);
    hop(meta, a, b);
    hop(meta, b, c);

    WorkflowMetaLayout.layout(meta);

    for (int i = 0; i < meta.nrActions(); i++) {
      assertOnGrid(meta.getAction(i).getLocation(), 16);
    }
  }

  @Test
  public void testSubsetAnchoredLayoutStaysOnGrid() {
    WorkflowMeta meta = new WorkflowMeta();
    ActionMeta a = action("a");
    ActionMeta b = action("b");
    ActionMeta other = action("other");
    a.setLocation(1001, 2003); // off-grid
    b.setLocation(3011, 61);
    other.setLocation(77, 88);
    meta.addAction(a);
    meta.addAction(b);
    meta.addAction(other);
    hop(meta, a, b);

    WorkflowMetaLayout.layout(meta, new LayeredGraphLayout.Options(), Arrays.asList(a, b));

    assertOnGrid(a.getLocation(), 16);
    assertOnGrid(b.getLocation(), 16);

    // The unselected action must not have moved.
    assertEquals(77, other.getLocation().x);
    assertEquals(88, other.getLocation().y);
  }

  @Test
  public void testLargeGridStartsOneCellInsideCanvas() {
    WorkflowMeta meta = new WorkflowMeta();
    ActionMeta a = action("a");
    ActionMeta b = action("b");
    meta.addAction(a);
    meta.addAction(b);
    hop(meta, a, b);

    // A grid larger than the margins used to round the first position down to (0,0).
    WorkflowMetaLayout.layout(meta, new LayeredGraphLayout.Options().setGridSize(128));

    assertEquals(128, a.getLocation().x); // first position: grid cell 1:1, not the origin
    assertEquals(128, a.getLocation().y);
    assertOnGrid(b.getLocation(), 128);
    assertTrue(b.getLocation().x > a.getLocation().x);
  }

  @Test
  public void testEmptyWorkflowDoesNotThrow() {
    WorkflowMetaLayout.layout(new WorkflowMeta());
  }

  @Test
  public void testNullDoesNotThrow() {
    WorkflowMetaLayout.layout(null);
  }

  @Test
  public void testSingleActionDoesNotThrow() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.addAction(action("only"));
    WorkflowMetaLayout.layout(meta);
  }

  @Test
  public void testCyclicWorkflowDoesNotThrow() {
    WorkflowMeta meta = new WorkflowMeta();
    ActionMeta a = action("a");
    ActionMeta b = action("b");
    ActionMeta c = action("c");
    meta.addAction(a);
    meta.addAction(b);
    meta.addAction(c);
    hop(meta, a, b);
    hop(meta, b, c);
    hop(meta, c, a); // cycle

    WorkflowMetaLayout.layout(meta);

    Set<String> coords = new HashSet<>();
    for (int i = 0; i < meta.nrActions(); i++) {
      Point p = meta.getAction(i).getLocation();
      assertTrue(coords.add(p.x + ":" + p.y));
    }
  }

  @Test
  public void testDisconnectedComponentsDoNotOverlap() {
    WorkflowMeta meta = new WorkflowMeta();
    ActionMeta a = action("a");
    ActionMeta b = action("b");
    ActionMeta x = action("x"); // separate component
    ActionMeta y = action("y");
    meta.addAction(a);
    meta.addAction(b);
    meta.addAction(x);
    meta.addAction(y);
    hop(meta, a, b);
    hop(meta, x, y);

    WorkflowMetaLayout.layout(meta);

    Set<String> coords = new HashSet<>();
    for (int i = 0; i < meta.nrActions(); i++) {
      Point p = meta.getAction(i).getLocation();
      assertTrue(coords.add(p.x + ":" + p.y));
    }
  }
}
