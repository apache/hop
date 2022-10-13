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
 *
 */

package org.apache.hop.ui.hopgui.perspective.execution;

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.*;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ToolBar;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class BaseExecutionViewer extends Composite
    implements KeyListener, MouseListener {

  public static final String STRING_STATE_STALE = "Stale";

  protected final HopGui hopGui;
  protected final PropsUi props;
  protected final int iconSize;
  protected final List<AreaOwner> areaOwners;

  protected ToolBar toolBar;
  protected GuiToolbarWidgets toolBarWidgets;
  protected SashForm sash;
  protected Canvas canvas;
  protected float magnification = 1.0f;
  protected CTabFolder tabFolder;
  protected Point offset;

  public BaseExecutionViewer(Composite parent, HopGui hopGui) {
    super(parent, SWT.NO_BACKGROUND);
    this.hopGui = hopGui;
    this.props = PropsUi.getInstance();
    this.iconSize = hopGui.getProps().getIconSize();
    this.areaOwners = new ArrayList<>();
    this.offset = new Point(0, 0);
  }

  protected Display hopDisplay() {
    return hopGui.getDisplay();
  }

  @Override
  public boolean setFocus() {
    return canvas.setFocus();
  }

  protected float calculateCorrectedMagnification() {
    return (float) (magnification * PropsUi.getInstance().getZoomFactor());
  }

  public Point screen2real(int x, int y) {
    float correctedMagnification = calculateCorrectedMagnification();
    Point real;
    real =
        new Point(
            Math.round((x / correctedMagnification - offset.x)),
            Math.round((y / correctedMagnification - offset.y)));

    return real;
  }

  public synchronized AreaOwner getVisibleAreaOwner(int x, int y) {
    for (int i = areaOwners.size() - 1; i >= 0; i--) {
      AreaOwner areaOwner = areaOwners.get(i);
      if (areaOwner.contains(x, y)) {
        return areaOwner;
      }
    }
    return null;
  }

  protected String formatDate(Date date) {
    if (date == null) {
      return "";
    }
    return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date);
  }

  public abstract void drillDownOnLocation(Point location);

  @Override
  public void keyPressed(KeyEvent keyEvent) {
  }

  @Override
  public void keyReleased(KeyEvent keyEvent) {}

  @Override
  public void mouseDoubleClick(MouseEvent mouseEvent) {
      drillDownOnLocation(screen2real(mouseEvent.x, mouseEvent.y));
  }

  /**
   * Gets toolBarWidgets
   *
   * @return value of toolBarWidgets
   */
  public GuiToolbarWidgets getToolBarWidgets() {
    return toolBarWidgets;
  }
}
