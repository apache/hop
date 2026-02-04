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
import org.eclipse.swt.widgets.Composite;

/**
 * Callback used by {@link IToolbarContainer} to add one toolbar item. The container passes its
 * control (a ToolBar or a Composite); the registrar branches on type so widget creation stays in
 * one place (e.g. {@link GuiToolbarWidgets}).
 */
public interface IToolbarWidgetRegistrar {

  /**
   * Add one toolbar item. {@code parent} is either an SWT ToolBar (desktop) or a Composite with
   * RowLayout (Hop Web); ToolBar extends Composite so both use the same callback.
   */
  void addItem(GuiToolbarItem item, Composite parent);
}
