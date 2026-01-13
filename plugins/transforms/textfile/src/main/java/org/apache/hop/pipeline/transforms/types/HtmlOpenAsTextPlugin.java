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
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileType;

@GuiPlugin
public class HtmlOpenAsTextPlugin {

  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = "ExplorerPerspective-Html-OpenAsText",
      label = "i18n:org.apache.hop.pipeline.transforms.types:HtmlOpenAsTextPlugin.OpenAsText.Label",
      image = "textfile.svg",
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      separator = true)
  public void openAsText() {
    ExplorerPerspective perspective = ExplorerPerspective.getInstance();
    ExplorerFile explorerFile = perspective.getSelectedFile();

    if (explorerFile == null) {
      return;
    }

    String filename = explorerFile.getFilename();
    if (filename == null || !filename.toLowerCase().endsWith(".html")) {
      return;
    }

    try {
      HopGui hopGui = HopGui.getInstance();

      // Find the text file type handler (by using a dummy .txt filename)
      IHopFileType hopFileType = HopFileTypeRegistry.getInstance().findHopFileType("dummy.txt");

      if (hopFileType instanceof IExplorerFileType) {
        IExplorerFileType textFileType = (IExplorerFileType) hopFileType;

        // Create a new ExplorerFile structure but force it to be the Text file type
        ExplorerFile textExplorerFile = new ExplorerFile();
        String uniqueName = filename + " (Text)";
        textExplorerFile.setFilename(uniqueName);
        textExplorerFile.setName(explorerFile.getName() + " (Text)");
        textExplorerFile.setFileType(textFileType);

        // Create the handler with overridden logic to handle the filename check
        TextExplorerFileTypeHandler handler =
            new TextExplorerFileTypeHandler(hopGui, perspective, textExplorerFile) {
              @Override
              public void reload() {
                // Swap filename to real path for loading
                String temp = textExplorerFile.getFilename();
                textExplorerFile.setFilename(filename);
                try {
                  super.reload();
                } finally {
                  textExplorerFile.setFilename(temp);
                }
              }

              @Override
              public void save() throws org.apache.hop.core.exception.HopException {
                // Swap filename to real path for saving
                String temp = textExplorerFile.getFilename();
                textExplorerFile.setFilename(filename);
                try {
                  super.save();
                } finally {
                  textExplorerFile.setFilename(temp);
                }
              }
            };

        // Add to perspective (this will open a new tab due to unique filename)
        perspective.addFile(handler);
      }
    } catch (Exception e) {
      HopGui.getInstance().getLog().logError("Error opening file as text", e);
    }
  }
}
