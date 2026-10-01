/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.ui.core.gui;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.eclipse.swt.widgets.MenuItem;
import org.junit.jupiter.api.Test;

class GuiMenuWidgetsDisposedTest {

  @Test
  void ignoresDisposedMenuItemWhenUpdatingCapability() {
    String id = "file-save";
    String permission = "save";
    IHopFileType fileType = mock(IHopFileType.class);
    MenuItem menuItem = mock(MenuItem.class);
    when(fileType.hasCapability(permission)).thenReturn(true);
    when(menuItem.isDisposed()).thenReturn(true);

    GuiMenuWidgets widgets = new GuiMenuWidgets();
    widgets.setMenuItemMap(Map.of(id, menuItem));

    widgets.enableMenuItem(fileType, id, permission);

    verify(menuItem, never()).isEnabled();
    verify(menuItem, never()).setEnabled(true);
  }
}
