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

package org.apache.hop.core.gui.markdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.NotePadType;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.markdown.MarkdownNoteRenderer.LayoutResult;
import org.apache.hop.core.gui.markdown.MarkdownNoteRenderer.PositionedLink;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.jupiter.api.Test;

class MarkdownNoteRendererTest {

  @Test
  void measureProducesPositiveSizeForHeadingsAndLists() {
    NotePadMeta note = new NotePadMeta();
    note.setMarkdown(true);
    note.setNoteType(NotePadType.INFORMATION);
    note.setNote(
        """
        # Title
        Some **bold** and *italic* text with a [link](https://hop.apache.org).

        - item one
        - item two

        1. first
        2. second
        """);

    LayoutResult result =
        MarkdownNoteRenderer.measure(new StubGc(), note, "Sans", 10, 0, null, null);
    assertTrue(result.width() > 20);
    assertTrue(result.height() > 20);
  }

  @Test
  void paintCollectsLinkHitsWithBounds() {
    NotePadMeta note = new NotePadMeta();
    note.setMarkdown(true);
    note.setNoteType(NotePadType.GENERAL);
    note.setNote("See [docs](https://hop.apache.org) and [pipeline](other.hpl).");

    List<PositionedLink> links = new ArrayList<>();
    LayoutResult result =
        MarkdownNoteRenderer.paintWithLinkBounds(
            new StubGc(), note, "Sans", 10, 10, 10, 400, null, links, null, null);

    assertTrue(result.width() > 0);
    assertEquals(2, links.size());
    assertEquals("https://hop.apache.org", links.get(0).hit().target());
    assertEquals("other.hpl", links.get(1).hit().target());
    assertTrue(links.get(0).width() > 0);
    assertTrue(links.get(0).height() > 0);
  }

  @Test
  void tableMarkdownMeasuresWithoutError() {
    NotePadMeta note = new NotePadMeta();
    note.setMarkdown(true);
    note.setNote("""
        | A | B |
        | --- | --- |
        | 1 | 2 |
        """);
    LayoutResult result =
        MarkdownNoteRenderer.measure(new StubGc(), note, "Sans", 10, 300, null, null);
    assertTrue(result.width() > 0);
    assertTrue(result.height() > 0);
  }

  @Test
  void emptyNoteReturnsMinimum() {
    NotePadMeta note = new NotePadMeta();
    note.setMarkdown(true);
    note.setNote("");
    LayoutResult result =
        MarkdownNoteRenderer.measure(new StubGc(), note, "Sans", 10, 0, null, null);
    assertEquals(20, result.width());
    assertEquals(20, result.height());
    assertTrue(result.links().isEmpty());
  }

  @Test
  void networkImageFallsBackToPlaceholder() {
    NotePadMeta note = new NotePadMeta();
    note.setMarkdown(true);
    note.setNote("![remote](https://example.com/x.png)");
    LayoutResult result =
        MarkdownNoteRenderer.measure(new StubGc(), note, "Sans", 10, 200, null, null);
    assertTrue(result.height() > 0);
    assertTrue(result.width() > 0);
  }

  @Test
  void notePadMetaDefaultsMarkdownAndType() {
    NotePadMeta note = new NotePadMeta();
    assertTrue(note.isMarkdown());
    assertEquals(NotePadType.GENERAL, note.getNoteType());
  }

  @Test
  void notePadMetaCopyPreservesMarkdownFields() {
    NotePadMeta source = new NotePadMeta("text", 1, 2, 3, 4);
    source.setMarkdown(true);
    source.setNoteType(NotePadType.WARNING);
    NotePadMeta copy = new NotePadMeta(source);
    assertTrue(copy.isMarkdown());
    assertEquals(NotePadType.WARNING, copy.getNoteType());
  }

  @Test
  void notePadTypeLookup() {
    assertEquals(NotePadType.IMPORTANT, NotePadType.lookupCode("IMPORTANT"));
    assertEquals(NotePadType.WARNING, NotePadType.lookupDescription("Warning"));
    assertFalse(NotePadType.getDescriptions().length < 4);
  }

  /** Minimal IGc that returns fixed text metrics for headless unit tests. */
  private static final class StubGc implements IGc {
    @Override
    public void setLineWidth(int width) {}

    @Override
    public void setFont(EFont font) {}

    @Override
    public int getFontHeight() {
      return 10;
    }

    @Override
    public Point textExtent(String text) {
      int len = text != null ? text.length() : 0;
      return new Point(Math.max(len * 7, 1), 14);
    }

    @Override
    public Point getDeviceBounds() {
      return new Point(1000, 1000);
    }

    @Override
    public void setBackground(EColor color) {}

    @Override
    public void setForeground(EColor color) {}

    @Override
    public void setBackground(int red, int green, int blue) {}

    @Override
    public void setForeground(int red, int green, int blue) {}

    @Override
    public void fillRectangle(int x, int y, int width, int height) {}

    @Override
    public void fillGradientRectangle(int x, int y, int width, int height, boolean vertical) {}

    @Override
    public void drawImage(EImage image, int x, int y, float magnification) {}

    @Override
    public void drawImage(EImage image, int x, int y, float magnification, double angle) {}

    @Override
    public void drawImage(
        SvgFile svgFile,
        int x,
        int y,
        int desiredWidth,
        int desiredHeight,
        float magnification,
        double angle) {}

    @Override
    public boolean drawFileImage(String path, int x, int y, int width, int height) {
      return false;
    }

    @Override
    public void drawLine(int x, int y, int x2, int y2) {}

    @Override
    public void setLineStyle(ELineStyle lineStyle) {}

    @Override
    public void drawRectangle(int x, int y, int width, int height) {}

    @Override
    public void drawPoint(int x, int y) {}

    @Override
    public void drawText(String text, int x, int y) {}

    @Override
    public void drawText(String text, int x, int y, boolean transparent) {}

    @Override
    public void fillRoundRectangle(
        int x, int y, int width, int height, int circleWidth, int circleHeight) {}

    @Override
    public void drawRoundRectangle(
        int x, int y, int width, int height, int circleWidth, int circleHeight) {}

    @Override
    public void fillPolygon(int[] polygon) {}

    @Override
    public void drawPolygon(int[] polygon) {}

    @Override
    public void drawPolyline(int[] polyline) {}

    @Override
    public void setAntialias(boolean antiAlias) {}

    @Override
    public void setTransform(float translationX, float translationY, float magnification) {}

    @Override
    public float getMagnification() {
      return 1.0f;
    }

    @Override
    public void setAlpha(int alpha) {}

    @Override
    public void dispose() {}

    @Override
    public int getAlpha() {
      return 255;
    }

    @Override
    public void setFont(String fontName, int fontSize, boolean fontBold, boolean fontItalic) {}

    @Override
    public void switchForegroundBackgroundColors() {}

    @Override
    public Point getArea() {
      return new Point(1000, 1000);
    }

    @Override
    public void drawTransformIcon(int x, int y, TransformMeta transformMeta, float magnification) {}

    @Override
    public void drawActionIcon(int x, int y, ActionMeta actionMeta, float magnification) {}
  }
}
