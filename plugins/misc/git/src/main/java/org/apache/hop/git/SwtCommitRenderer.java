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
 *
 */

package org.apache.hop.git;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revplot.AbstractPlotRenderer;
import org.eclipse.jgit.revplot.PlotCommit;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Event;

public class SwtCommitRenderer extends AbstractPlotRenderer<SwtCommitList.Lane, Color> {
  private static final int MAX_LABEL_LENGTH = 18;
  private static final String ELLIPSIS = "…";

  private GC gc;
  @Getter @Setter private String currentBranch;
  private Color foregroundColor;
  private Color backgroundColor;
  private int cx;
  private int cy;
  private boolean isHead;
  @Getter @Setter private boolean filtered;

  public SwtCommitRenderer() {
    this.filtered = false;
  }

  void paintCommit(Event event, PlotCommit<SwtCommitList.Lane> commit) {
    this.gc = event.gc;
    this.foregroundColor = gc.getForeground();
    this.backgroundColor = gc.getBackground();
    this.cx = event.x;
    this.cy = event.y;

    // Check if this is a HEAD commit
    this.isHead = false;
    for (int i = 0; i < commit.getRefCount(); i++) {
      Ref ref = commit.getRef(i);
      if (ref.getName().equals(Constants.HEAD)) {
        isHead = true;
        break;
      }
    }

    paintCommit(commit, event.height);
  }

  void paintWorkingDirectory(Event event) {
    this.gc = event.gc;
    this.cx = event.x;
    this.cy = event.y;

    int size = 16;
    int x = 0;
    int y = (event.height - size) / 2 + 1;
    gc.setForeground(GuiResource.getInstance().getColorGray());
    gc.setBackground(GuiResource.getInstance().getColorGray());
    drawBoundaryDot(x, y, size, size);
  }

  @Override
  protected void drawLine(Color color, int x1, int y1, int x2, int y2, int width) {
    gc.setForeground(color);
    gc.setBackground(color);
    gc.setLineStyle(filtered ? SWT.LINE_DASH : SWT.LINE_SOLID);
    gc.setLineWidth(width);
    gc.drawLine(cx + x1, cy + y1, cx + x2, cy + y2);
  }

  @Override
  protected void drawCommitDot(int x, int y, int w, int h) {
    if (isHead) {
      Color color = gc.getBackground();
      gc.fillOval(cx + x - 2, cy + y - 3, w + 6, h + 6);
      gc.setBackground(backgroundColor);
      gc.fillOval(cx + x + 1, cy + y, w, h);
      gc.setBackground(color);
    } else {
      gc.fillOval(cx + x + 1, cy + y, w, h);
    }
  }

  @Override
  protected void drawBoundaryDot(int x, int y, int w, int h) {
    gc.setLineWidth(2);
    gc.setLineStyle(SWT.LINE_DOT);
    gc.drawOval(cx + x + 1, cy + y + 1, w, h);
    gc.setLineStyle(SWT.LINE_SOLID);
    gc.fillOval(cx + x + 4, cy + y + 4, 10, 10);
  }

  @Override
  protected int drawLabel(int x, int y, Ref ref) {

    // Ignore symbolic label like HEAD
    if (ref.isSymbolic()) {
      return 0;
    }

    String name = ref.getName();
    boolean isTag = false;

    if (name.startsWith(Constants.R_HEADS)) {
      name = name.substring(Constants.R_HEADS.length());
    } else if (name.startsWith(Constants.R_REMOTES)) {
      name = name.substring(Constants.R_REMOTES.length());
    } else if (name.startsWith(Constants.R_TAGS)) {
      isTag = true;
      name = name.substring(Constants.R_TAGS.length());
    } else {
      // Whatever this would be
      if (name.startsWith(Constants.R_REFS)) name = name.substring(Constants.R_REFS.length());
    }

    if (name.length() > MAX_LABEL_LENGTH) {
      name = name.substring(0, MAX_LABEL_LENGTH) + ELLIPSIS;
    }

    Font font = gc.getFont();
    if (name.equals(currentBranch)) {
      gc.setFont(GuiResource.getInstance().getFontBold());
    }

    // Compute text size with the current font
    Point textExtent = gc.textExtent(name);

    int w = textExtent.x + 3;

    Color foregroundColor = gc.getForeground();
    // Draw tag
    if (isTag) {
      gc.fillRoundRectangle(cx + x, cy + 2, w, textExtent.y - 2, 6, 6);
      gc.setForeground(GuiResource.getInstance().getColorWhite());
    }
    // Draw branch
    else {
      gc.drawRoundRectangle(cx + x, cy + 2, w, textExtent.y - 2, 6, 6);
    }

    gc.drawString(name, cx + x + 2, cy, true);
    gc.setFont(font);
    gc.setForeground(foregroundColor);

    return w + 4;
  }

  @Override
  protected Color laneColor(SwtCommitList.Lane lane) {
    return lane.getColor();
  }

  @Override
  protected void drawText(String text, int x, int y) {
    // gc.drawText(text, x, y, true);
  }
}
