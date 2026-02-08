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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/** Hop Web (RAP) toolbar container: wraps a Composite with RowLayout (wrap). */
public class WebToolbarContainer implements IToolbarContainer {

  private final Composite composite;

  public WebToolbarContainer(Composite composite) {
    this.composite = composite;
  }

  @Override
  public Control getControl() {
    return composite;
  }

  @Override
  public void addItem(GuiToolbarItem item, IToolbarWidgetRegistrar registrar) {
    registrar.addItem(item, composite);
  }
}
