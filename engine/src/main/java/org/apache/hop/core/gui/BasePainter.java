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
import org.apache.hop.core.gui.IGc.EFont;
import org.apache.hop.core.gui.IGc.EImage;
import org.apache.hop.core.gui.IGc.ELineStyle;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Getter
@Setter
public abstract class BasePainter<Hop extends BaseHopMeta<?>, Part extends IBaseMeta> {

  private static final Class<?> PKG = BasePainter.class;

  public final double theta = Math.toRadians(11); // arrowhead sharpness

  public static final int MINI_ICON_MARGIN = 5;
  public static final int CORNER_RADIUS_5 = 10;

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

  /**
   * When true, draw the origin boundary (dashed lines, hatching, and label). Should be set from the
   * UI based on "Enable infinite move" (only show when infinite move is enabled).
   */
  protected boolean showOriginBoundary;

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
        (int) (fontHeight / zoomFactor),
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
    gc.setForeground(EColor.BLACK);
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

  /**
   * Maximum grid points to draw; avoids severe slowdown on large/zoomed-out canvases (e.g.
   * Windows).
   */
  private static final int MAX_GRID_POINTS = 50_000;

  protected void drawGrid() {
    if (area == null || area.x <= 0 || area.y <= 0) {
      return;
    }
    // Grid is drawn in the same coordinate system as drawOriginBoundary: the origin (0,0) of the
    // pipeline is at (offset.x, offset.y) here. The hatched "outside workable" area is x < offset.x
    // or y < offset.y. So we only draw grid where x >= offset.x and y >= offset.y.
    float mag = Math.max(0.1f, magnification);
    int originX = (int) Math.round(offset.x);
    int originY = (int) Math.round(offset.y);
    int workableMinX = Math.max(0, originX);
    int workableMinY = Math.max(0, originY);
    // Visible extent in this coordinate system is (0,0) to (area.x/mag, area.y/mag); workable part
    // is from (originX, originY) to that right/bottom edge.
    int workableMaxX = (int) Math.ceil(area.x / mag);
    int workableMaxY = (int) Math.ceil(area.y / mag);
    if (workableMaxX <= workableMinX || workableMaxY <= workableMinY) {
      return;
    }

    int baseStep = (mag < 1.0f) ? Math.max(gridSize, (int) (gridSize / mag)) : gridSize;
    int rangeX = workableMaxX - workableMinX;
    int rangeY = workableMaxY - workableMinY;
    long totalPoints = (long) (rangeX / baseStep + 1) * (rangeY / baseStep + 1);
    int step = baseStep;
    if (totalPoints > MAX_GRID_POINTS) {
      int minStep =
          Math.max(
              gridSize,
              (int) Math.ceil(Math.sqrt((double) rangeX * rangeY / (double) MAX_GRID_POINTS)));
      step = Math.max(baseStep, minStep);
    }
    int offsetX = (int) (offset.x % step);
    int offsetY = (int) (offset.y % step);
    if (offsetX < 0) {
      offsetX += step;
    }
    if (offsetY < 0) {
      offsetY += step;
    }
    // First grid position at or after workable visible origin (never in hatched area)
    int startX =
        Math.max(
            workableMinX,
            offsetX + step * (int) Math.ceil((double) (workableMinX - offsetX) / step));
    int startY =
        Math.max(
            workableMinY,
            offsetY + step * (int) Math.ceil((double) (workableMinY - offsetY) / step));
    for (int x = startX; x < workableMaxX; x += step) {
      for (int y = startY; y < workableMaxY; y += step) {
        if (x >= 0 && y >= 0) {
          gc.drawPoint(x, y);
        }
      }
    }
  }

  /**
   * Draws a subtle boundary at the origin (0,0) and hatches the negative coordinate space to
   * indicate that transforms/actions cannot be created there. Only drawn when showOriginBoundary is
   * true (e.g. when "Enable infinite move" is on). Same coordinate system as drawGrid (real = graph
   * + offset).
   */
  protected void drawOriginBoundary() {
    if (!showOriginBoundary) {
      return;
    }
    double visW = area.x / Math.max(0.01, magnification);
    double visH = area.y / Math.max(0.01, magnification);
    int ox = (int) Math.round(offset.x);
    int oy = (int) Math.round(offset.y);

    int alpha = gc.getAlpha();
    gc.setAlpha(160);

    // Hatch the negative region as an L-shape so the corner is not double-hatched:
    // 1) Full strip left of origin: [0, ox] x [0, visH]
    // 2) Top strip above origin only to the right of the left strip: [ox, visW] x [0, oy]
    if (ox > 0 && oy > 0) {
      drawHatching(0, 0, ox, (int) visH); // left strip (full height)
      int topW = (int) visW - ox;
      if (topW > 0 && oy > 0) {
        drawHatching(ox, 0, topW, oy); // top strip (no overlap)
      }
    } else if (ox > 0 && ox < visW) {
      drawHatching(0, 0, ox, (int) visH);
    } else if (oy > 0 && oy < visH) {
      drawHatching(0, 0, (int) visW, oy);
    }

    gc.setAlpha(alpha);

    // Dashed border on top of the hatching: only the positive side of the axes (actionable area),
    // so the line does not extend into the negative/hatched region
    gc.setForeground(EColor.BLACK);
    gc.setLineWidth(1);
    gc.setLineStyle(ELineStyle.DASH);

    if (ox >= 0 && ox <= visW && oy <= visH) {
      gc.drawLine(ox, oy, ox, (int) visH); // vertical segment from origin downward
    }
    if (oy >= 0 && oy <= visH && ox <= visW) {
      gc.drawLine(ox, oy, (int) visW, oy); // horizontal segment from origin rightward
    }

    gc.setLineStyle(ELineStyle.SOLID);

    // "Outside workable area" label on the x-axis and y-axis (both normal/horizontal)
    String originBoundaryLabel = BaseMessages.getString(PKG, "BasePainter.OutsideWorkableArea");
    if (originBoundaryLabel != null && !originBoundaryLabel.isEmpty()) {
      gc.setForeground(EColor.DARKGRAY);
      gc.setFont(EFont.SMALL);
      Point textSize = gc.textExtent(originBoundaryLabel);
      int tw = textSize.x;
      int th = textSize.y;
      if (ox > tw && ox < visW && (int) visH > th) {
        int tx = Math.max(4, (ox - tw) / 2);
        int ty = ((int) visH - th) / 2;
        gc.drawText(originBoundaryLabel, tx, ty);
      }
      if (oy > th && oy < visH && (int) visW - ox > tw) {
        int topW = (int) visW - ox;
        int tx = ox + (topW - tw) / 2;
        int ty = Math.max(4, (oy - th) / 2);
        gc.drawText(originBoundaryLabel, tx, ty);
      }
    }
  }

  /**
   * Base spacing between hatching lines at 100% zoom; scaled by zoom so we draw fewer when zoomed
   * out.
   */
  private static final int HATCH_STEP_BASE = 24;

  /**
   * Draws subtle diagonal hatching in the given rectangle. Uses a global diagonal grid (lines x+y =
   * base + n*step) so that when multiple regions are hatched (e.g. left and top of origin), the
   * lines align seamlessly across the boundary.
   */
  private void drawHatching(int x, int y, int w, int h) {
    if (w <= 0 || h <= 0) {
      return;
    }
    gc.setForeground(EColor.DARKGRAY);
    gc.setLineWidth(1);
    gc.setLineStyle(ELineStyle.SOLID);
    float mag = Math.max(0.1f, magnification);
    int step = Math.max(HATCH_STEP_BASE, (int) (HATCH_STEP_BASE / mag));
    // Global base so all hatched regions share the same grid (aligns at boundaries).
    int base = (int) (((offset.x + offset.y) % step + step) % step);
    // Diagonals are lines where x+y = K; they intersect the rect when K is in [x+y, x+w+y+h].
    int kMin = x + y;
    int kMax = x + w + y + h;
    int nStart = (int) Math.ceil((double) (kMin - base) / step);
    int nEnd = (int) Math.floor((double) (kMax - base) / step);
    for (int n = nStart; n <= nEnd; n++) {
      int k = base + n * step;
      // Clip line x+y=k to rect [x,x+w] x [y,y+h]: x in [max(x, k-(y+h)), min(x+w, k-y)].
      int x1 = Math.max(x, k - (y + h));
      int x2 = Math.min(x + w, k - y);
      if (x1 <= x2) {
        gc.drawLine(x1, k - x1, x2, k - x2);
      }
    }
  }

  protected int round(double value) {
    return (int) Math.round(value);
  }

  protected int calcArrowLength() {
    return 19 + (lineWidth - 1) * 5; // arrowhead length
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

  /** Fixed width of the navigation viewport (minimap) in pixels. */
  private static final int VIEWPORT_WIDTH = 200;

  /** Maximum height of the minimap; height follows content aspect ratio up to this cap. */
  private static final int VIEWPORT_HEIGHT_MAX = 200;

  /**
   * Draw a small rectangle at the bottom right of the screen which depicts the viewport as a part
   * of the total size of the metadata graph. Width is fixed; height follows content aspect ratio
   * (capped) so the minimap represents the graph without spurious empty space. Content is top-left
   * aligned so "at the top" of the canvas matches the top of the minimap.
   */
  protected void drawNavigationView() {
    if (!showingNavigationView || maximum == null) {
      return;
    }
    double contentMaxX = Math.max(1, maximum.x);
    double contentMaxY = Math.max(1, maximum.y);

    // 1) Visible rectangle in graph coordinates (unclamped so we can show panned-outside view).
    //
    double mag = Math.max(0.01, magnification);
    double visibleLeft = -offset.x;
    double visibleTop = -offset.y;
    double visibleWidthGraph = area.x / mag;
    double visibleHeightGraph = area.y / mag;
    double visibleRightGraph = visibleLeft + visibleWidthGraph;
    double visibleBottomGraph = visibleTop + visibleHeightGraph;

    // 2) Minimap bounds = union of content and visible view so the overlay always fits inside.
    //
    double minGraphX = Math.min(0, visibleLeft);
    double minGraphY = Math.min(0, visibleTop);
    double maxGraphX = Math.max(contentMaxX, visibleRightGraph);
    double maxGraphY = Math.max(contentMaxY, visibleBottomGraph);
    double graphRangeX = Math.max(1, maxGraphX - minGraphX);
    double graphRangeY = Math.max(1, maxGraphY - minGraphY);

    // 3) Fixed width; height follows content aspect ratio (capped) so the minimap fits the graph.
    //
    int viewportHeight =
        Math.min(
            VIEWPORT_HEIGHT_MAX,
            Math.max(20, (int) Math.round(VIEWPORT_WIDTH * graphRangeY / graphRangeX)));
    double scale = Math.min(VIEWPORT_WIDTH / graphRangeX, viewportHeight / graphRangeY);
    double contentWidth = graphRangeX * scale;
    double contentHeight = graphRangeY * scale;
    double contentLeft = area.x - VIEWPORT_WIDTH - 10.0;
    double contentTop = area.y - viewportHeight - 10.0;
    // Top-left align so "at the top" of the canvas matches the top of the minimap

    int alpha = gc.getAlpha();
    gc.setAlpha(75);

    gc.setForeground(EColor.DARKGRAY);
    gc.setBackground(EColor.LIGHTBLUE);
    gc.drawRectangle((int) contentLeft, (int) contentTop, VIEWPORT_WIDTH, viewportHeight);
    gc.fillRectangle((int) contentLeft, (int) contentTop, VIEWPORT_WIDTH, viewportHeight);

    // 3) Origin for graph (0,0) in minimap pixels; content and overlay use same scale.
    //
    double graphOriginX = contentLeft - minGraphX * scale;
    double graphOriginY = contentTop - minGraphY * scale;
    drawNavigationViewContent(graphOriginX, graphOriginY, scale, scale);

    double viewX = graphOriginX + visibleLeft * scale;
    double viewY = graphOriginY + visibleTop * scale;
    double viewWidth = visibleWidthGraph * scale;
    double viewHeight = visibleHeightGraph * scale;

    // Clamp overlay to the drawn content area (avoid drawing outside the light blue)
    viewX = Math.max(contentLeft, Math.min(contentLeft + contentWidth - 1, viewX));
    viewY = Math.max(contentTop, Math.min(contentTop + contentHeight - 1, viewY));
    viewWidth = Math.min(viewWidth, contentLeft + contentWidth - viewX);
    viewHeight = Math.min(viewHeight, contentTop + contentHeight - viewY);
    viewWidth = Math.max(0, viewWidth);
    viewHeight = Math.max(0, viewHeight);

    gc.setForeground(EColor.BLACK);
    gc.setBackground(EColor.BLUE);
    gc.drawRectangle((int) viewX, (int) viewY, (int) viewWidth, (int) viewHeight);
    gc.fillRectangle((int) viewX, (int) viewY, (int) viewWidth, (int) viewHeight);

    graphPort = new Rectangle((int) contentLeft, (int) contentTop, VIEWPORT_WIDTH, viewportHeight);
    viewPort = new Rectangle((int) viewX, (int) viewY, (int) viewWidth, (int) viewHeight);

    gc.setAlpha(alpha);
  }

  /**
   * Override to draw rectangles and lines inside the navigation viewport representing the graph
   * elements (e.g. transforms/actions and hops). Coordinates are in graph space; convert to
   * viewport pixels with: screenX = graphX + graphCoordX * scaleX, screenY = graphY + graphCoordY *
   * scaleY.
   *
   * @param graphX left of the viewport rectangle in screen pixels
   * @param graphY top of the viewport rectangle in screen pixels
   * @param scaleX scale from graph X to viewport width
   * @param scaleY scale from graph Y to viewport height
   */
  protected void drawNavigationViewContent(
      double graphX, double graphY, double scaleX, double scaleY) {
    // Default: nothing. PipelinePainter and WorkflowPainter draw transforms/actions and hops.
  }
}
