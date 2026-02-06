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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.ToolBar;

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
}
