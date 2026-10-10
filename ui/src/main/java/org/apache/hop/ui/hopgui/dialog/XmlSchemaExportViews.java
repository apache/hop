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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.ui.hopgui.HopGui;

/** Adds "Generate XML Schemas..." to the Tools menu in HopGui. */
@GuiPlugin
public class XmlSchemaExportViews {

  public static final Class<?> PKG = XmlSchemaExportViews.class;

  public static final String ID_MAIN_MENU_TOOLS_GENERATE_SCHEMAS =
      "40040-menu-tools-generate-xml-schemas";

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_GENERATE_SCHEMAS,
      label = "i18n::XmlSchemaExportViews.Menu.Tools.GenerateSchemas",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      separator = true,
      image = "ui/images/metadata.svg")
  public void menuToolsGenerateXmlSchemas() {
    HopGui hopGui;
    try {
      hopGui = HopGui.peekInstance();
    } catch (Throwable e) {
      return;
    }
    if (hopGui != null) {
      XmlSchemaExportDialog dialog =
          new XmlSchemaExportDialog(hopGui.getShell(), hopGui.getVariables());
      dialog.open();
    }
  }
}
