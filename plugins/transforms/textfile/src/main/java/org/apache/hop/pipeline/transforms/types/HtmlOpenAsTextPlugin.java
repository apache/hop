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

package org.apache.hop.pipeline.transforms.types;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.raw.RawExplorerFileType;

@GuiPlugin
public class HtmlOpenAsTextPlugin {

  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = ExplorerPerspective.CONTEXT_MENU_OPEN_AS_TEXT,
      label =
          "i18n:org.apache.hop.ui.hopgui.perspective.explorer:ExplorerPerspective.ToolbarElement.OpenAsText.Label",
      image = "textfile.svg",
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID)
  public void openAsText() {
    ExplorerPerspective perspective = ExplorerPerspective.getInstance();
    ExplorerFile explorerFile = perspective.getSelectedFile();

    if (explorerFile == null) {
      return;
    }

    String filename = explorerFile.getFilename();
    if (filename == null) {
      return;
    }

    try {
      if (HopVfs.getFileObject(filename).isFolder()) {
        return;
      }
    } catch (Exception e) {
      HopGui.getInstance().getLog().logError("Error resolving selected item", e);
      return;
    }

    try {
      HopGui hopGui = HopGui.getInstance();
      RawExplorerFileType rawFileType = new RawExplorerFileType();
      rawFileType.openFile(hopGui, filename, hopGui.getVariables());
    } catch (Exception e) {
      HopGui.getInstance().getLog().logError("Error opening file as text", e);
    }
  }
}
