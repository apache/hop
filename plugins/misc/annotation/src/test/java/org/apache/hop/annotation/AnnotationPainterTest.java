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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.annotation.AnnotationGeometry.Bounds;
import org.apache.hop.annotation.AnnotationGeometry.Segment;
import org.apache.hop.core.NotePadType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.NotePadStyle;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.pipeline.PipelinePainterExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowPainterExtension;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AnnotationPainterTest {

  @AfterEach
  void resetDarkMode() {
    NotePadStyle.setDarkMode(false);
  }

  @Test
  void paintsACalloutOnlyWhenThereIsText() {
    NotePadStyle.setDarkMode(false);
    TransformMeta transform = new TransformMeta();
    transform.setSelected(true);
    Annotation annotation = new Annotation();
    annotation.setText("Hi");
    annotation.setPlacement(AnnotationPlacement.TOP);

    StubGc gc = new StubGc();
    List<AreaOwner> areas = new ArrayList<>();
    DPoint offset = new DPoint(4, 6);
    AnnotationPainter.paint(gc, areas, offset, transform, 100, 80, 32, true, annotation);

    assertEquals(IGc.EFont.NOTE, gc.font);
    assertEquals(NotePadStyle.backgroundColor(NotePadType.GENERAL).red(), gc.backgroundRed);
    assertEquals(230, gc.backgroundRed);
    assertEquals(2, gc.roundRectLineWidth);
    assertEquals(1, areas.size());
    AreaOwner area = areas.get(0);
    assertEquals(AreaOwner.AreaType.CUSTOM, area.getAreaType());
    AnnotationHit hit = assertInstanceOf(AnnotationHit.class, area.getParent());
    assertSame(transform, hit.subject());

    Bounds expected =
        AnnotationGeometry.balloon(
            new Bounds(100, 80, 32, 32), gc.textExtent("Hi").x, 10, AnnotationPlacement.TOP, 1f);
    assertEquals(expected.x(), gc.roundX);
    assertEquals(expected.y(), gc.roundY);
    assertEquals(expected.x() - 4, area.getArea().x);
    assertEquals(expected.y() - 6, area.getArea().y);
    assertEquals(expected.width(), area.getArea().width);
    assertTrue(gc.drawnText.contains("Hi"));
    assertEquals(1, gc.lineCount);
    assertEquals(IGc.ELineStyle.DASH, gc.drawnLineStyle);
    Segment connector = AnnotationGeometry.connector(expected, new Bounds(100, 80, 32, 32));
    assertEquals(connector.x1(), gc.lineX1);
    assertEquals(connector.y1(), gc.lineY1);
    assertEquals(connector.x2(), gc.lineX2);
    assertEquals(connector.y2(), gc.lineY2);
    assertEquals(IGc.ELineStyle.SOLID, gc.lineStyle);

    areas.clear();
    annotation.setText("  ");
    AnnotationPainter.paint(gc, areas, offset, transform, 100, 80, 32, false, annotation);
    assertTrue(areas.isEmpty());
  }

  @Test
  void darkModeUsesTheDarkGeneralPalette() {
    NotePadStyle.setDarkMode(true);
    ActionMeta action = new ActionMeta(new ActionDummy("end"));
    Annotation annotation = new Annotation();
    annotation.setText("End");
    List<AreaOwner> areas = new ArrayList<>();
    StubGc gc = new StubGc();

    AnnotationPainter.paint(gc, areas, new DPoint(0, 0), action, 0, 0, 32, false, annotation);

    assertEquals(NotePadStyle.backgroundColor(NotePadType.GENERAL).red(), gc.backgroundRed);
    assertEquals(55, gc.backgroundRed);
    assertEquals(1, gc.roundRectLineWidth);
    assertEquals(1, areas.size());
  }

  @Test
  void dashedLineCanBeTurnedOff() {
    Annotation annotation = new Annotation();
    annotation.setText("Hi");
    annotation.setDashedLine(false);
    StubGc gc = new StubGc();
    AnnotationPainter.paint(
        gc, new ArrayList<>(), new DPoint(0, 0), new TransformMeta(), 0, 0, 32, false, annotation);
    assertEquals(0, gc.lineCount);
  }

  @Test
  void calloutSizeAndClickAreaDoNotDependOnMagnification() {
    // 25 characters fit in MAX_TEXT_WIDTH_PX (25 * 7 = 175). The whole line is 217, which fits
    // only when the wrap width is scaled by the zoom a second time.
    String text = "1234567890123456789012345 extra";
    Annotation annotation = new Annotation();
    annotation.setText(text);
    annotation.setDashedLine(false);
    annotation.setPlacement(AnnotationPlacement.TOP);
    DPoint offset = new DPoint(4, 6);
    Bounds icon = new Bounds(100, 80, 32, 32);

    AreaOwner first = null;
    for (float magnification : new float[] {0.5f, 1f, 2f}) {
      StubGc gc = new StubGc();
      gc.magnification = magnification;
      List<AreaOwner> areas = new ArrayList<>();
      AnnotationPainter.paint(
          gc, areas, offset, new TransformMeta(), 100, 80, 32, false, annotation);

      assertEquals(2, gc.drawnText.size(), "magnification " + magnification);
      Bounds expected =
          AnnotationGeometry.balloon(icon, 25 * 7, 2 * 10, AnnotationPlacement.TOP, magnification);
      assertEquals(expected.x(), gc.roundX);
      assertEquals(expected.y(), gc.roundY);
      assertEquals(expected.width(), gc.roundWidth);
      assertEquals(expected.height(), gc.roundHeight);
      assertEquals(AnnotationGeometry.RADIUS_PX, gc.roundRadius);
      assertEquals(expected.x() + AnnotationGeometry.PADDING_PX, gc.textX);
      assertEquals(expected.y() + AnnotationGeometry.PADDING_PX, gc.textY);
      assertEquals(25 * 7 + 2 * AnnotationGeometry.PADDING_PX, expected.width());
      assertEquals(2 * 10 + 2 * AnnotationGeometry.PADDING_PX, expected.height());

      AreaOwner area = areas.get(0);
      assertEquals(expected.width(), area.getArea().width);
      assertEquals(expected.height(), area.getArea().height);
      assertEquals(expected.x() - 4, area.getArea().x);
      assertEquals(expected.y() - 6, area.getArea().y);

      // screen2real: graph = canvas / magnification - offset. The GC transform drew the balloon
      // at canvas = magnification * draw, and the click rectangle is draw minus offset.
      double canvasX = magnification * expected.centerX();
      double canvasY = magnification * expected.centerY();
      int graphX = (int) Math.round(canvasX / magnification - offset.x);
      int graphY = (int) Math.round(canvasY / magnification - offset.y);
      assertTrue(area.contains(graphX, graphY), "magnification " + magnification);
      assertFalse(area.contains(area.getArea().x - 1, graphY));

      if (first == null) {
        first = area;
      } else {
        assertEquals(first.getArea().x, area.getArea().x);
        assertEquals(first.getArea().y, area.getArea().y);
        assertEquals(first.getArea().width, area.getArea().width);
        assertEquals(first.getArea().height, area.getArea().height);
      }
    }
  }

  @Test
  void painterExtensionReadsTheStoredAnnotation() throws HopException {
    TransformMeta transform = new TransformMeta();
    Annotation annotation = new Annotation();
    annotation.setText("In");
    annotation.setPlacement(AnnotationPlacement.RIGHT);
    AnnotationStore.write(transform, annotation);

    List<AreaOwner> areas = new ArrayList<>();
    PipelinePainterExtension pipeline =
        new PipelinePainterExtension(
            new StubGc(),
            areas,
            null,
            transform,
            null,
            10,
            20,
            0,
            0,
            0,
            0,
            new DPoint(0, 0),
            32,
            null);
    new DrawTransformAnnotationExtensionPoint().callExtensionPoint(null, null, pipeline);
    assertEquals(1, areas.size());

    ActionMeta action = new ActionMeta(new ActionDummy("go"));
    AnnotationStore.write(action, annotation);
    List<AreaOwner> actionAreas = new ArrayList<>();
    WorkflowPainterExtension workflow =
        new WorkflowPainterExtension(
            new StubGc(),
            actionAreas,
            null,
            null,
            action,
            10,
            20,
            0,
            0,
            0,
            0,
            new DPoint(0, 0),
            32);
    new DrawActionAnnotationExtensionPoint().callExtensionPoint(null, null, workflow);
    assertEquals(1, actionAreas.size());
    assertSame(action, ((AnnotationHit) actionAreas.get(0).getParent()).subject());
  }

  /** Minimal IGc that records the callout colors and shape. */
  private static final class StubGc implements IGc {
    private int lineWidth = 1;
    private float magnification = 1f;
    private IGc.EFont font;
    private int backgroundRed = -1;
    private int roundX;
    private int roundY;
    private int roundWidth;
    private int roundHeight;
    private int roundRadius;
    private int textX;
    private int textY;
    private boolean textPositionRecorded;
    private int roundRectLineWidth = -1;
    private final List<String> drawnText = new ArrayList<>();
    private IGc.ELineStyle lineStyle = IGc.ELineStyle.SOLID;
    private IGc.ELineStyle drawnLineStyle;
    private int lineCount;
    private int lineX1;
    private int lineY1;
    private int lineX2;
    private int lineY2;

    @Override
    public void setLineWidth(int width) {
      lineWidth = width;
    }

    @Override
    public void setFont(EFont font) {
      this.font = font;
    }

    @Override
    public int getFontHeight() {
      return 10;
    }

    @Override
    public Point textExtent(String text) {
      int length = text == null ? 0 : text.length();
      return new Point(Math.max(length * 7, 1), 14);
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
    public void setBackground(int red, int green, int blue) {
      backgroundRed = red;
    }

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
    public void drawLine(int x, int y, int x2, int y2) {
      lineCount++;
      drawnLineStyle = lineStyle;
      lineX1 = x;
      lineY1 = y;
      lineX2 = x2;
      lineY2 = y2;
    }

    @Override
    public void setLineStyle(ELineStyle lineStyle) {
      this.lineStyle = lineStyle;
    }

    @Override
    public void drawRectangle(int x, int y, int width, int height) {}

    @Override
    public void drawPoint(int x, int y) {}

    @Override
    public void drawText(String text, int x, int y) {
      recordText(text, x, y);
    }

    @Override
    public void drawText(String text, int x, int y, boolean transparent) {
      recordText(text, x, y);
    }

    private void recordText(String text, int x, int y) {
      if (!textPositionRecorded) {
        textX = x;
        textY = y;
        textPositionRecorded = true;
      }
      drawnText.add(text);
    }

    @Override
    public void fillRoundRectangle(
        int x, int y, int width, int height, int circleWidth, int circleHeight) {
      roundX = x;
      roundY = y;
      roundWidth = width;
      roundHeight = height;
      roundRadius = circleWidth;
    }

    @Override
    public void drawRoundRectangle(
        int x, int y, int width, int height, int circleWidth, int circleHeight) {
      roundRectLineWidth = lineWidth;
    }

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
      return magnification;
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
