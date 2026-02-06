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

package org.apache.hop.ui.core.gui;

import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.eclipse.swt.widgets.Control;

/**
 * Abstraction over a toolbar container: either an SWT ToolBar (desktop) or a Composite with
 * RowLayout (Hop Web). Allows adding toolbar items in a single code path; the implementation
 * handles the difference.
 */
public interface IToolbarContainer {

  /** The control to use for layout (setLayoutData, etc.). */
  Control getControl();

  /**
   * Add one toolbar item (separator, optional label, and the element). The implementation creates
   * the appropriate widgets and registers them via the registrar.
   */
  void addItem(GuiToolbarItem item, IToolbarWidgetRegistrar registrar);
}
