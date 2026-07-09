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

package org.apache.hop.ui.hopgui;

import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.gui.IToolbarWidgetRegistrar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/** Desktop (RCP) toolbar container: wraps an SWT ToolBar. */
public class ToolBarToolbarContainer implements IToolbarContainer {

  private final ToolBar toolBar;

  public ToolBarToolbarContainer(ToolBar toolBar) {
    this.toolBar = toolBar;
  }

  @Override
  public Control getControl() {
    return toolBar;
  }

  @Override
  public void addItem(GuiToolbarItem item, IToolbarWidgetRegistrar registrar) {
    registrar.addItem(item, toolBar);
  }

  @Override
  public void enableWrapAutoHeight() {
    // A SWT.WRAP toolbar wraps its items to extra rows when it is too narrow, but it does not
    // grow its own allocated height to match. On every resize we measure the real extent of the
    // (wrapped) items and feed it back into the FormData height so the layout gives the toolbar
    // enough room and pushes the content below it down.
    toolBar.addListener(SWT.Resize, event -> adjustWrappedHeight());
  }

  private void adjustWrappedHeight() {
    if (toolBar.isDisposed() || !(toolBar.getLayoutData() instanceof FormData formData)) {
      return;
    }
    int contentHeight = 0;
    for (ToolItem item : toolBar.getItems()) {
      Rectangle bounds = item.getBounds();
      contentHeight = Math.max(contentHeight, bounds.y + bounds.height);
    }
    // Nothing to measure yet, or the height already matches: avoid a redundant re-layout (which
    // would otherwise loop, since re-laying out fires another Resize event).
    if (contentHeight <= 0 || formData.height == contentHeight) {
      return;
    }
    formData.height = contentHeight;
    Composite parent = toolBar.getParent();
    if (parent != null && !parent.isDisposed()) {
      parent.layout(true);
    }
  }
}
