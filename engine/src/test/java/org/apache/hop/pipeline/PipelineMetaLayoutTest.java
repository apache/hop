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

    // The near note moved by the same delta as transform 'a'.
    aDx = a.getLocation().x - aDx;
    aDy = a.getLocation().y - aDy;
    assertEquals(110 + aDx, nearNote.getLocation().x);
    assertEquals(110 + aDy, nearNote.getLocation().y);

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

    // The arranged block is anchored to the top-left of where the subset was (minX=1000, minY=50).
    int minX = Math.min(a.getLocation().x, b.getLocation().x);
    int minY = Math.min(a.getLocation().y, b.getLocation().y);
    assertEquals(1000, minX);
    assertEquals(50, minY);

    // And it still reads left-to-right.
    assertTrue(b.getLocation().x > a.getLocation().x, "subset not left-to-right");
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
