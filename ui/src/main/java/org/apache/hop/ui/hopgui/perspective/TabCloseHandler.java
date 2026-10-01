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

package org.apache.hop.ui.hopgui.perspective;

import lombok.Getter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

public class TabCloseHandler {

  private static final Class<?> PKG = HopGui.class;

  private final TabClosable tabClosablePerspective;
  CTabFolder tabFolder;

  /**
   * The tab the context menu was opened on. It is set before the menu is shown, so listeners on the
   * menu can rely on it being current for this click.
   */
  @Getter CTabItem selectedItem;

  /**
   * The tab context menu. It is not attached to the folder, so callers that want to add their own
   * items have to get it from here rather than from {@code CTabFolder.getMenu()}.
   */
  @Getter private final Menu menu;

  public TabCloseHandler(TabClosable tabClosablePerspective) {
    this(tabClosablePerspective, tabClosablePerspective.getTabFolder());
  }

  public TabCloseHandler(TabClosable tabClosablePerspective, CTabFolder tabFolder) {
    this.tabClosablePerspective = tabClosablePerspective;
    this.tabFolder = tabFolder;

    // The menu is shown from MenuDetect, and only when the click lands on a tab. It is not attached
    // with setMenu on purpose: the Hop Web client shows an attached menu by itself, wherever in
    // the folder the right click lands, before the server can veto it.
    menu = new Menu(tabFolder);
    tabFolder.addListener(SWT.MenuDetect, this::handleTabMenuDetectEvent);
    tabFolder.addListener(SWT.MouseUp, event -> handleMouseUp(event, tabClosablePerspective));

    MenuItem miClose = new MenuItem(menu, SWT.NONE);
    miClose.setText(BaseMessages.getString(PKG, "HopGui.TabItem.Close.Text"));
    miClose.addListener(
        SWT.Selection, event -> tabClosablePerspective.closeTab(null, selectedItem));

    MenuItem miCloseOthers = new MenuItem(menu, SWT.NONE);
    miCloseOthers.setText(BaseMessages.getString(PKG, "HopGui.TabItem.CloseOther.Text"));
    miCloseOthers.addListener(
        SWT.Selection,
        event ->
            tabClosablePerspective
                .getOtherTabs(selectedItem)
                .forEach(tabItem -> tabClosablePerspective.closeTab(null, tabItem)));

    MenuItem miCloseAll = new MenuItem(menu, SWT.NONE);
    miCloseAll.setText(BaseMessages.getString(PKG, "HopGui.TabItem.CloseAll.Text"));
    miCloseAll.addListener(
        SWT.Selection,
        event -> {
          for (CTabItem tabItem : this.tabFolder.getItems()) {
            tabClosablePerspective.closeTab(null, tabItem);
          }
        });

    MenuItem miCloseLeft = new MenuItem(menu, SWT.NONE);
    miCloseLeft.setText(BaseMessages.getString(PKG, "HopGui.TabItem.CloseLeft.Text"));
    miCloseLeft.addListener(
        SWT.Selection,
        event ->
            tabClosablePerspective
                .getTabsToLeft(selectedItem)
                .forEach(tabItem -> tabClosablePerspective.closeTab(null, tabItem)));

    MenuItem miCloseRight = new MenuItem(menu, SWT.NONE);
    miCloseRight.setText(BaseMessages.getString(PKG, "HopGui.TabItem.CloseRight.Text"));
    miCloseRight.addListener(
        SWT.Selection,
        event ->
            tabClosablePerspective
                .getTabsToRight(selectedItem)
                .forEach(tabItem -> tabClosablePerspective.closeTab(null, tabItem)));
  }

  private void handleMouseUp(Event event, TabClosable tabClosablePerspective) {

    // Middle button close tab
    if (event.button == 2) {
      Point point = new Point(event.x, event.y);
      CTabItem item = tabFolder.getItem(point);
      if (item != null) {
        tabClosablePerspective.closeTab(null, item);
      }
    }

    event.doit = false;
  }

  private void handleTabMenuDetectEvent(Event event) {
    // The event carries the click as display coordinates. Display.getCursorLocation() is only as
    // fresh as the last mouse event the server saw, which in Hop Web need not be this click.
    Point point = tabFolder.toControl(event.x, event.y);
    selectedItem = tabFolder.getItem(point);
    if (selectedItem != null) {
      menu.setLocation(event.x, event.y);
      menu.setVisible(true);
    }
  }
}
