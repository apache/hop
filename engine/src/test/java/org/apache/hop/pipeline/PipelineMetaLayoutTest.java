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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.layout.LayeredGraphLayout;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

public class PipelineMetaLayoutTest {

  private TransformMeta transform(String name) {
    TransformMeta t = new TransformMeta();
    t.setName(name);
    t.setLocation(0, 0);
    return t;
  }

  private void hop(PipelineMeta meta, TransformMeta from, TransformMeta to) {
    meta.addPipelineHop(new PipelineHopMeta(from, to));
  }

  private void assertOnGrid(Point p, int gridSize) {
    assertEquals(0, Math.floorMod(p.x, gridSize), "x not on grid: " + p.x);
    assertEquals(0, Math.floorMod(p.y, gridSize), "y not on grid: " + p.y);
  }

  @Test
  public void testLayoutNoOverlapAndLeftToRight() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta c = transform("c");
    TransformMeta d = transform("d"); // fan: a -> d, then d -> c (join into c)
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    meta.addTransform(d);
    hop(meta, a, b);
    hop(meta, b, c);
    hop(meta, a, d);
    hop(meta, d, c);

    PipelineMetaLayout.layout(meta);

    // (a) no two transforms share the same (x,y)
    Set<String> coords = new HashSet<>();
    for (int i = 0; i < meta.nrTransforms(); i++) {
      Point p = meta.getTransform(i).getLocation();
      assertTrue(coords.add(p.x + ":" + p.y), "Duplicate coordinate at " + p.x + "," + p.y);
    }

    // (b) every forward hop goes left-to-right
    for (int i = 0; i < meta.nrPipelineHops(); i++) {
      PipelineHopMeta h = meta.getPipelineHop(i);
      Point from = h.getFromTransform().getLocation();
      Point to = h.getToTransform().getLocation();
      assertTrue(
          to.x > from.x,
          h.getFromTransform().getName()
              + " -> "
              + h.getToTransform().getName()
              + " not left-to-right ("
              + from.x
              + " >= "
              + to.x
              + ")");
    }
  }

  @Test
  public void testNoteFollowsNearestNodeButDistantNoteStaysPut() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    a.setLocation(100, 100);
    b.setLocation(120, 5000); // far away on the canvas
    meta.addTransform(a);
    meta.addTransform(b);
    hop(meta, a, b);

    // A note right next to 'a', and a note far from everything.
    NotePadMeta nearNote = new NotePadMeta("near a", 110, 110, 50, 20);
    NotePadMeta farNote = new NotePadMeta("orphan", 9000, 9000, 50, 20);
    meta.addNote(nearNote);
    meta.addNote(farNote);

    int aDx = a.getLocation().x; // captured before layout
    int aDy = a.getLocation().y;

    PipelineMetaLayout.layout(meta, new LayeredGraphLayout.Options());

    // The near note follows transform 'a' and, like a manual move, lands on the grid.
    aDx = a.getLocation().x - aDx;
    aDy = a.getLocation().y - aDy;
    assertOnGrid(nearNote.getLocation(), 16);
    assertTrue(Math.abs(nearNote.getLocation().x - (110 + aDx)) <= 8);
    assertTrue(Math.abs(nearNote.getLocation().y - (110 + aDy)) <= 8);

    // The far note was left untouched.
    assertEquals(9000, farNote.getLocation().x);
    assertEquals(9000, farNote.getLocation().y);
  }

  @Test
  public void testMoveNotesDisabledLeavesNotesAlone() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    a.setLocation(100, 100);
    b.setLocation(200, 100);
    meta.addTransform(a);
    meta.addTransform(b);
    hop(meta, a, b);
    NotePadMeta note = new NotePadMeta("near a", 110, 110, 50, 20);
    meta.addNote(note);

    PipelineMetaLayout.layout(meta, new LayeredGraphLayout.Options().setMoveNotes(false));

    assertEquals(110, note.getLocation().x);
    assertEquals(110, note.getLocation().y);
  }

  @Test
  public void testTopBottomDirectionFlowsDownward() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta c = transform("c");
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    hop(meta, a, b);
    hop(meta, b, c);

    PipelineMetaLayout.layout(
        meta,
        new LayeredGraphLayout.Options().setDirection(LayeredGraphLayout.Direction.TOP_BOTTOM));

    // Every forward hop goes top-to-bottom.
    for (int i = 0; i < meta.nrPipelineHops(); i++) {
      PipelineHopMeta h = meta.getPipelineHop(i);
      assertTrue(
          h.getToTransform().getLocation().y > h.getFromTransform().getLocation().y,
          "hop not top-to-bottom");
    }
  }

  @Test
  public void testSelectionSubsetLeavesUnselectedInPlaceAndAnchors() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta other = transform("other");
    a.setLocation(1000, 2000);
    b.setLocation(3000, 50);
    other.setLocation(77, 88);
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(other);
    hop(meta, a, b);

    // Lay out only {a, b}; 'other' is not part of the subset.
    PipelineMetaLayout.layout(meta, new LayeredGraphLayout.Options(), Arrays.asList(a, b));

    // The unselected transform must not have moved.
    assertEquals(77, other.getLocation().x);
    assertEquals(88, other.getLocation().y);

    // The arranged block is anchored near the top-left of where the subset was (minX=1000,
    // minY=50), snapped to the grid so later manual moves stay aligned with it.
    int minX = Math.min(a.getLocation().x, b.getLocation().x);
    int minY = Math.min(a.getLocation().y, b.getLocation().y);
    assertOnGrid(a.getLocation(), 16);
    assertOnGrid(b.getLocation(), 16);
    assertTrue(Math.abs(minX - 1000) <= 8);
    assertTrue(Math.abs(minY - 50) <= 8);

    // And it still reads left-to-right.
    assertTrue(b.getLocation().x > a.getLocation().x, "subset not left-to-right");
  }

  @Test
  public void testPositionsAlignToDefaultGrid() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta c = transform("c");
    a.setLocation(101, 203); // deliberately off-grid origins
    b.setLocation(302, 51);
    c.setLocation(17, 19);
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    hop(meta, a, b);
    hop(meta, b, c);

    PipelineMetaLayout.layout(meta);

    for (int i = 0; i < meta.nrTransforms(); i++) {
      assertOnGrid(meta.getTransform(i).getLocation(), 16);
    }
  }

  @Test
  public void testPositionsAlignToCustomGridSize() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta c = transform("c");
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    hop(meta, a, b);
    hop(meta, b, c);

    PipelineMetaLayout.layout(
        meta,
        new LayeredGraphLayout.Options().setLayerSpacing(151).setNodeSpacing(97).setGridSize(10));

    for (int i = 0; i < meta.nrTransforms(); i++) {
      assertOnGrid(meta.getTransform(i).getLocation(), 10);
    }
  }

  @Test
  public void testGridSizeOneDisablesSnapping() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta c = transform("c");
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    hop(meta, a, b);
    hop(meta, b, c);

    PipelineMetaLayout.layout(
        meta, new LayeredGraphLayout.Options().setLayerSpacing(151).setGridSize(1));

    // Raw margins and spacing: nothing is rounded to a grid.
    assertEquals(50, a.getLocation().x);
    assertEquals(50, a.getLocation().y);
    assertEquals(201, b.getLocation().x);
    assertEquals(50, b.getLocation().y);
    assertEquals(352, c.getLocation().x);
    assertEquals(50, c.getLocation().y);
  }

  @Test
  public void testSubsetAnchoredLayoutStaysOnGrid() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta other = transform("other");
    a.setLocation(1001, 2003); // off-grid
    b.setLocation(3011, 61);
    other.setLocation(77, 88);
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(other);
    hop(meta, a, b);

    PipelineMetaLayout.layout(meta, new LayeredGraphLayout.Options(), Arrays.asList(a, b));

    assertOnGrid(a.getLocation(), 16);
    assertOnGrid(b.getLocation(), 16);

    // The unselected transform must not have moved.
    assertEquals(77, other.getLocation().x);
    assertEquals(88, other.getLocation().y);
  }

  @Test
  public void testLargeGridStartsOneCellInsideCanvas() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    meta.addTransform(a);
    meta.addTransform(b);
    hop(meta, a, b);

    // A grid larger than the margins used to round the first position down to (0,0).
    PipelineMetaLayout.layout(meta, new LayeredGraphLayout.Options().setGridSize(128));

    assertEquals(128, a.getLocation().x); // first position: grid cell 1:1, not the origin
    assertEquals(128, a.getLocation().y);
    assertOnGrid(b.getLocation(), 128);
    assertTrue(b.getLocation().x > a.getLocation().x);
  }

  @Test
  public void testSubsetNearCornerStaysOneCellInsideCanvas() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    a.setLocation(5, 5);
    b.setLocation(200, 10);
    meta.addTransform(a);
    meta.addTransform(b);
    hop(meta, a, b);

    PipelineMetaLayout.layout(meta, new LayeredGraphLayout.Options(), Arrays.asList(a, b));

    // The anchored block never lands on the grid origin or off-canvas.
    assertOnGrid(a.getLocation(), 16);
    assertOnGrid(b.getLocation(), 16);
    assertTrue(a.getLocation().x >= 16);
    assertTrue(a.getLocation().y >= 16);
    assertTrue(b.getLocation().x > a.getLocation().x);
  }

  @Test
  public void testEmptyPipelineDoesNotThrow() {
    PipelineMetaLayout.layout(new PipelineMeta());
  }

  @Test
  public void testNullDoesNotThrow() {
    PipelineMetaLayout.layout(null);
  }

  @Test
  public void testSingleTransformDoesNotThrow() {
    PipelineMeta meta = new PipelineMeta();
    meta.addTransform(transform("only"));
    PipelineMetaLayout.layout(meta);
  }

  @Test
  public void testCyclicPipelineDoesNotThrow() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta c = transform("c");
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    hop(meta, a, b);
    hop(meta, b, c);
    hop(meta, c, a); // cycle

    PipelineMetaLayout.layout(meta);

    // No duplicate coordinates even with a cycle.
    Set<String> coords = new HashSet<>();
    for (int i = 0; i < meta.nrTransforms(); i++) {
      Point p = meta.getTransform(i).getLocation();
      assertTrue(coords.add(p.x + ":" + p.y));
    }
  }

  @Test
  public void testDisconnectedComponentsDoNotOverlap() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("a");
    TransformMeta b = transform("b");
    TransformMeta x = transform("x"); // separate component
    TransformMeta y = transform("y");
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(x);
    meta.addTransform(y);
    hop(meta, a, b);
    hop(meta, x, y);

    PipelineMetaLayout.layout(meta);

    Set<String> coords = new HashSet<>();
    for (int i = 0; i < meta.nrTransforms(); i++) {
      Point p = meta.getTransform(i).getLocation();
      assertTrue(coords.add(p.x + ":" + p.y));
    }
  }
}
