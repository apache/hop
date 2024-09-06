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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

public class TabCloseHandler {

  private static final Class<?> PKG = HopDataOrchestrationPerspective.class;

  CTabFolder tabFolder;
  CTabItem selectedItem;

  public TabCloseHandler(TabClosable tabClosablePerspective) {
    this.tabFolder = tabClosablePerspective.getTabFolder();

    Menu menu = new Menu(tabFolder);
    tabFolder.setMenu(menu);
    tabFolder.addListener(SWT.MenuDetect, this::handleTabMenuDetectEvent);
    tabFolder.addListener(SWT.MouseUp, event -> handleMouseUp(event, tabClosablePerspective));

    // Create menu item
    MenuItem miClose = new MenuItem(menu, SWT.NONE);
    miClose.setText(BaseMessages.getString(PKG, "DataOrchestrationPerspective.Close.Button.Text"));
    miClose.addListener(
        SWT.Selection, event -> tabClosablePerspective.closeTab(null, selectedItem));

    MenuItem miCloseOthers = new MenuItem(menu, SWT.NONE);
    miCloseOthers.setText(
        BaseMessages.getString(PKG, "DataOrchestrationPerspective.CloseOther.Button.Text"));
    miCloseOthers.addListener(
        SWT.Selection,
        event ->
            tabClosablePerspective
                .getOtherTabs(selectedItem)
                .forEach(tabItem -> tabClosablePerspective.closeTab(null, tabItem)));

    MenuItem miCloseAll = new MenuItem(menu, SWT.NONE);
    miCloseAll.setText(
        BaseMessages.getString(PKG, "DataOrchestrationPerspective.CloseAll.Button.Text"));
    miCloseAll.addListener(
        SWT.Selection,
        event -> {
          for (CTabItem tabItem : tabClosablePerspective.getTabFolder().getItems()) {
            tabClosablePerspective.closeTab(null, tabItem);
          }
        });

    MenuItem miCloseLeft = new MenuItem(menu, SWT.NONE);
    miCloseLeft.setText(
        BaseMessages.getString(PKG, "DataOrchestrationPerspective.CloseLeft.Button.Text"));
    miCloseLeft.addListener(
        SWT.Selection,
        event ->
            tabClosablePerspective
                .getTabsToLeft(selectedItem)
                .forEach(tabItem -> tabClosablePerspective.closeTab(null, tabItem)));

    MenuItem miCloseRight = new MenuItem(menu, SWT.NONE);
    miCloseRight.setText(
        BaseMessages.getString(PKG, "DataOrchestrationPerspective.CloseRight.Button.Text"));
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
    Point point = tabFolder.toControl(tabFolder.getDisplay().getCursorLocation());
    selectedItem = tabFolder.getItem(new Point(point.x, point.y));

    if (selectedItem == null) {
      event.doit = false;
    }
  }
}
