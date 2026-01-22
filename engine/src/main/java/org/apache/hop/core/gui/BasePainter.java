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

package org.apache.hop.core.gui;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.base.IBaseMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.IGc.EColor;
import org.apache.hop.core.gui.IGc.EImage;
import org.apache.hop.core.gui.IGc.ELineStyle;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

public abstract class BasePainter<Hop extends BaseHopMeta<?>, Part extends IBaseMeta> {

  public final double theta = Math.toRadians(11); // arrowhead sharpness

  public static final int MINI_ICON_MARGIN = 5;
  public static final int MINI_ICON_TRIANGLE_BASE = 20;
  public static final int MINI_ICON_DISTANCE = 4;
  public static final int MINI_ICON_SKEW = 0;

  public static final int CONTENT_MENU_INDENT = 4;

  public static final int CORNER_RADIUS_5 = 10;
  public static final int CORNER_RADIUS_4 = 8;
  public static final int CORNER_RADIUS_3 = 6;
  public static final int CORNER_RADIUS_2 = 4;

  protected boolean drawingBorderAroundName;

  protected double zoomFactor;

  protected Point area;

  protected List<AreaOwner> areaOwners;

  protected DPoint offset;
  protected int iconSize;
  protected int miniIconSize;
  protected int gridSize;
  protected Rectangle selectionRectangle;
  protected int lineWidth;
  protected float magnification;

  protected Object subject;
  protected IVariables variables;

  protected IGc gc;

  private String noteFontName;

  private int noteFontHeight;

  protected Hop candidate;

  protected Point maximum;
  protected boolean showingNavigationView;
  protected float screenMagnification;
  protected Rectangle graphPort;
  protected Rectangle viewPort;
  protected String mouseOverName;

  @Getter @Setter protected boolean darkMode;

  public BasePainter(
      IGc gc,
      IVariables variables,
      Object subject,
      Point area,
      DPoint offset,
      Rectangle selectionRectangle,
      List<AreaOwner> areaOwners,
      int iconSize,
      int lineWidth,
      int gridSize,
      String noteFontName,
      int noteFontHeight,
      double zoomFactor,
      boolean drawingBorderAroundName,
      String mouseOverName) {
    this.gc = gc;
    this.variables = variables;
    this.subject = subject;
    this.area = area;
    this.offset = offset;

    this.selectionRectangle = selectionRectangle;

    this.areaOwners = areaOwners;
    areaOwners.clear(); // clear it before we start filling it up again.

    this.iconSize = iconSize;
    this.miniIconSize = iconSize / 2;
    this.lineWidth = lineWidth;
    this.gridSize = gridSize;

    this.magnification = 1.0f;
    this.zoomFactor = zoomFactor;
    this.drawingBorderAroundName = drawingBorderAroundName;

    gc.setAntialias(true);

    this.noteFontName = noteFontName;
    this.noteFontHeight = noteFontHeight;

    this.screenMagnification = 1.0f;
    this.mouseOverName = mouseOverName;
  }

  public static EImage getStreamIconImage(StreamIcon streamIcon, boolean enabled) {
    return switch (streamIcon) {
      case TRUE -> (enabled) ? EImage.TRUE : EImage.TRUE_DISABLED;
      case FALSE -> (enabled) ? EImage.FALSE : EImage.FALSE_DISABLED;
      case ERROR -> (enabled) ? EImage.ERROR : EImage.ERROR_DISABLED;
      case INFO -> (enabled) ? EImage.INFO : EImage.INFO_DISABLED;
      case TARGET -> (enabled) ? EImage.TARGET : EImage.TARGET_DISABLED;
      case INPUT -> EImage.INPUT;
      case OUTPUT -> EImage.OUTPUT;
      default -> EImage.ARROW_DEFAULT;
    };
  }

  protected Point calculateMinimumSize(NotePadMeta note) {
    if (Utils.isEmpty(note.getNote())) {
      return new Point(20, 20); // Empty note
    }

    int fontHeight;
    if (note.getFontSize() > 0) {
      fontHeight = note.getFontSize();
    } else {
      fontHeight = noteFontHeight;
    }
    gc.setFont(
        Const.NVL(note.getFontName(), noteFontName),
        (int) ((double) fontHeight / zoomFactor),
        note.isFontBold(),
        note.isFontItalic());

    Point size = gc.textExtent(note.getNote());
    size.x += 2 * Const.NOTE_MARGIN;
    size.y += 2 * Const.NOTE_MARGIN;

    return size;
  }

  protected void drawNote(NotePadMeta noteMeta) {
    if (noteMeta.isSelected()) {
      gc.setLineWidth(2);
    } else {
      gc.setLineWidth(1);
    }

    Point minimumSize = this.calculateMinimumSize(noteMeta);

    // Cache the minimum size for resize operation
    noteMeta.setMinimumWidth(minimumSize.x);
    noteMeta.setMinimumHeight(minimumSize.y);

    Point loc = noteMeta.getLocation();
    Point note = real2screen(loc.x, loc.y);

    int width = noteMeta.width;
    int height = noteMeta.height;
    if (minimumSize.x > width) {
      width = minimumSize.x;
    }
    if (minimumSize.y > height) {
      height = minimumSize.y;
    }
    Rectangle noteShape = new Rectangle(note.x, note.y, width, height);

    gc.setBackground(
        noteMeta.getBackGroundColorRed(),
        noteMeta.getBackGroundColorGreen(),
        noteMeta.getBackGroundColorBlue());
    gc.setForeground(
        noteMeta.getBorderColorRed(),
        noteMeta.getBorderColorGreen(),
        noteMeta.getBorderColorBlue());

    // Radius is half the font height
    //
    int radius = (int) Math.round(zoomFactor * 8);

    gc.fillRoundRectangle(
        noteShape.x, noteShape.y, noteShape.width, noteShape.height, radius, radius);
    gc.drawRoundRectangle(
        noteShape.x, noteShape.y, noteShape.width, noteShape.height, radius, radius);

    if (!Utils.isEmpty(noteMeta.getNote())) {
      gc.setForeground(
          noteMeta.getFontColorRed(), noteMeta.getFontColorGreen(), noteMeta.getFontColorBlue());
      gc.drawText(noteMeta.getNote(), note.x + Const.NOTE_MARGIN, note.y + Const.NOTE_MARGIN, true);
    }

    // Add to the list of areas...
    //
    areaOwners.add(
        new AreaOwner(
            AreaType.NOTE,
            noteShape.x,
            noteShape.y,
            noteShape.width,
            noteShape.height,
            offset,
            subject,
            noteMeta));
  }

  protected Point real2screen(int x, int y) {
    return new Point((int) (x + offset.x), (int) (y + offset.y));
  }

  protected Point magnifyPoint(Point p) {
    return new Point(Math.round(p.x * magnification), Math.round(p.y * magnification));
  }

  protected void drawRect(Rectangle rect) {
    if (rect == null) {
      return;
    }
    gc.setLineStyle(ELineStyle.DASHDOT);
    gc.setLineWidth(lineWidth);
    gc.setForeground(EColor.GRAY);
    // SWT on Windows doesn't cater for negative rect.width/height so handle here.
    Point s = real2screen(rect.x, rect.y);
    if (rect.width < 0) {
      s.x = s.x + rect.width;
    }
    if (rect.height < 0) {
      s.y = s.y + rect.height;
    }
    gc.drawRectangle(s.x, s.y, Math.abs(rect.width), Math.abs(rect.height));
    gc.setLineStyle(ELineStyle.SOLID);
  }

  protected void drawGrid() {
    Point bounds = gc.getDeviceBounds();
    for (int x = 0; x < bounds.x; x += gridSize) {
      for (int y = 0; y < bounds.y; y += gridSize) {
        gc.drawPoint((int) (x + (offset.x % gridSize)), (int) (y + (offset.y % gridSize)));
      }
    }
  }

  protected int round(double value) {
    return (int) Math.round(value);
  }

  protected int calcArrowLength() {
    return 19 + (lineWidth - 1) * 5; // arrowhead length
  }

  /**
   * @return the magnification
   */
  public float getMagnification() {
    return magnification;
  }

  /**
   * @param magnification the magnification to set
   */
  public void setMagnification(float magnification) {
    this.magnification = magnification;
  }

  public Point getArea() {
    return area;
  }

  public void setArea(Point area) {
    this.area = area;
  }

  public List<AreaOwner> getAreaOwners() {
    return areaOwners;
  }

  public void setAreaOwners(List<AreaOwner> areaOwners) {
    this.areaOwners = areaOwners;
  }

  public DPoint getOffset() {
    return offset;
  }

  public void setOffset(DPoint offset) {
    this.offset = offset;
  }

  public int getIconSize() {
    return iconSize;
  }

  public void setIconSize(int iconSize) {
    this.iconSize = iconSize;
  }

  public int getGridSize() {
    return gridSize;
  }

  public void setGridSize(int gridSize) {
    this.gridSize = gridSize;
  }

  public Rectangle getSelectionRectangle() {
    return selectionRectangle;
  }

  public void setSelectionRectangle(Rectangle selectionRectangle) {
    this.selectionRectangle = selectionRectangle;
  }

  public int getLineWidth() {
    return lineWidth;
  }

  public void setLineWidth(int lineWidth) {
    this.lineWidth = lineWidth;
  }

  public Object getSubject() {
    return subject;
  }

  public void setSubject(Object subject) {
    this.subject = subject;
  }

  public IGc getGc() {
    return gc;
  }

  public void setGc(IGc gc) {
    this.gc = gc;
  }

  public String getNoteFontName() {
    return noteFontName;
  }

  public void setNoteFontName(String noteFontName) {
    this.noteFontName = noteFontName;
  }

  public int getNoteFontHeight() {
    return noteFontHeight;
  }

  public void setNoteFontHeight(int noteFontHeight) {
    this.noteFontHeight = noteFontHeight;
  }

  public double getTheta() {
    return theta;
  }

  public Hop getCandidate() {
    return candidate;
  }

  public void setCandidate(Hop candidate) {
    this.candidate = candidate;
  }

  protected int[] getLine(Part fs, Part ts) {
    if (fs == null || ts == null) {
      return null;
    }

    Point from = fs.getLocation();
    Point to = ts.getLocation();

    int x1 = from.x + iconSize / 2;
    int y1 = from.y + iconSize / 2;

    int x2 = to.x + iconSize / 2;
    int y2 = to.y + iconSize / 2;

    return new int[] {x1, y1, x2, y2};
  }

  protected void drawArrow(EImage arrow, int[] line, Hop hop, Object startObject, Object endObject)
      throws HopException {
    Point screenFrom = real2screen(line[0], line[1]);
    Point screenTo = real2screen(line[2], line[3]);

    drawArrow(
        arrow,
        screenFrom.x,
        screenFrom.y,
        screenTo.x,
        screenTo.y,
        theta,
        calcArrowLength(),
        -1,
        hop,
        startObject,
        endObject);
  }

  protected abstract void drawArrow(
      EImage arrow,
      int x1,
      int y1,
      int x2,
      int y2,
      double theta,
      int size,
      double factor,
      Hop jobHop,
      Object startObject,
      Object endObject)
      throws HopException;

  /**
   * Draw a small rectangle at the bottom right of the screen which depicts the viewport as a part
   * of the total size of the metadata graph. If the graph fits completely on the screen, the
   * navigation view is not shown.
   */
  protected void drawNavigationView() {
    if (!showingNavigationView || maximum == null) {
      // Disabled or no maximum size available
      return;
    }

    // Compensate the screen size for the current magnification
    //
    int areaWidth = (int) (area.x / magnification);
    int areaHeight = (int) (area.y / magnification);

    // We want to show a rectangle depicting the total area of the pipeline/workflow graph.
    // This area must be adjusted to a maximum of 200 if it is too large.
    //
    double graphWidth = maximum.x;
    double graphHeight = maximum.y;
    if (graphWidth > 200 || graphHeight > 200) {
      double coefficient = 200 / Math.max(graphWidth, graphHeight);
      graphWidth *= coefficient;
      graphHeight *= coefficient;
    }

    // Position it in the bottom right corner of the screen
    //
    double graphX = area.x - graphWidth - 10.0;
    double graphY = area.y - graphHeight - 10.0;

    int alpha = gc.getAlpha();
    gc.setAlpha(75);

    gc.setForeground(EColor.DARKGRAY);
    gc.setBackground(EColor.LIGHTBLUE);
    gc.drawRectangle((int) graphX, (int) graphY, (int) graphWidth, (int) graphHeight);
    gc.fillRectangle((int) graphX, (int) graphY, (int) graphWidth, (int) graphHeight);

    // Now draw a darker area inside showing the size of the view-screen in relation to the graph
    // surface. The view size is a fraction of the total graph area outlined above.
    //
    double viewWidth = (graphWidth * areaWidth) / Math.max(areaWidth, maximum.x);
    double viewHeight = (graphHeight * areaHeight) / Math.max(areaHeight, maximum.y);

    // The offset is a part of the screen size.  The maximum offset is the graph size minus the area
    // size.
    // The offset horizontally is [0, -maximum.x+areaWidth]
    // The idea is that if the right side of the pipeline or workflow is shown you don't need to
    // scroll further.
    // The offset fractions calculated below are in the range 0-1 (there about)
    //
    double offsetXFraction = (double) (-offset.x) / ((double) maximum.x);
    double offsetYFraction = (double) (-offset.y) / ((double) maximum.y);

    // We shift the view rectangle to the right or down based on the offset fraction and the wiggle
    // room of the inner rectangle.
    //
    double viewX = graphX + (graphWidth) * offsetXFraction;
    double viewY = graphY + (graphHeight) * offsetYFraction;

    gc.setForeground(EColor.BLACK);
    gc.setBackground(EColor.BLUE);
    gc.drawRectangle((int) viewX, (int) viewY, (int) viewWidth, (int) viewHeight);
    gc.fillRectangle((int) viewX, (int) viewY, (int) viewWidth, (int) viewHeight);

    // We remember the rectangles so that we can navigate in it when the user drags it around.
    //
    graphPort = new Rectangle((int) graphX, (int) graphY, (int) graphWidth, (int) graphHeight);
    viewPort = new Rectangle((int) viewX, (int) viewY, (int) viewWidth, (int) viewHeight);

    gc.setAlpha(alpha);
  }

  /**
   * Gets zoomFactor
   *
   * @return value of zoomFactor
   */
  public double getZoomFactor() {
    return zoomFactor;
  }

  /**
   * @param zoomFactor The zoomFactor to set
   */
  public void setZoomFactor(double zoomFactor) {
    this.zoomFactor = zoomFactor;
  }

  /**
   * Gets drawingEditIcons
   *
   * @return value of drawingBorderAroundName
   */
  public boolean isDrawingBorderAroundName() {
    return drawingBorderAroundName;
  }

  /**
   * @param drawingBorderAroundName The option to set
   */
  public void setDrawingBorderAroundName(boolean drawingBorderAroundName) {
    this.drawingBorderAroundName = drawingBorderAroundName;
  }

  /**
   * Gets miniIconSize
   *
   * @return value of miniIconSize
   */
  public int getMiniIconSize() {
    return miniIconSize;
  }

  /**
   * @param miniIconSize The miniIconSize to set
   */
  public void setMiniIconSize(int miniIconSize) {
    this.miniIconSize = miniIconSize;
  }

  /**
   * Gets showingNavigationView
   *
   * @return value of showingNavigationView
   */
  public boolean isShowingNavigationView() {
    return showingNavigationView;
  }

  /**
   * Sets showingNavigationView
   *
   * @param showingNavigationView value of showingNavigationView
   */
  public void setShowingNavigationView(boolean showingNavigationView) {
    this.showingNavigationView = showingNavigationView;
  }

  /**
   * Gets maximum
   *
   * @return value of maximum
   */
  public Point getMaximum() {
    return maximum;
  }

  /**
   * Sets maximum
   *
   * @param maximum value of maximum
   */
  public void setMaximum(Point maximum) {
    this.maximum = maximum;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * Sets variables
   *
   * @param variables value of variables
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets screenMagnification
   *
   * @return value of screenMagnification
   */
  public float getScreenMagnification() {
    return screenMagnification;
  }

  /**
   * Sets screenMagnification
   *
   * @param screenMagnification value of screenMagnification
   */
  public void setScreenMagnification(float screenMagnification) {
    this.screenMagnification = screenMagnification;
  }

  /**
   * Gets graphPort
   *
   * @return value of graphPort
   */
  public Rectangle getGraphPort() {
    return graphPort;
  }

  /**
   * Sets graphPort
   *
   * @param graphPort value of graphPort
   */
  public void setGraphPort(Rectangle graphPort) {
    this.graphPort = graphPort;
  }

  /**
   * Gets viewPort
   *
   * @return value of viewPort
   */
  public Rectangle getViewPort() {
    return viewPort;
  }

  /**
   * Sets viewPort
   *
   * @param viewPort value of viewPort
   */
  public void setViewPort(Rectangle viewPort) {
    this.viewPort = viewPort;
  }

  /**
   * Gets mouseOverName
   *
   * @return value of mouseOverName
   */
  public String getMouseOverName() {
    return mouseOverName;
  }

  /**
   * Sets mouseOverName
   *
   * @param mouseOverName value of mouseOverName
   */
  public void setMouseOverName(String mouseOverName) {
    this.mouseOverName = mouseOverName;
  }
}
