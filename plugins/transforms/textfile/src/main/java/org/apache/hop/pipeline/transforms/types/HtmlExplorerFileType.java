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

package org.apache.hop.pipeline.transforms.types;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.capabilities.FileTypeCapabilities;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileType;

@HopFileTypePlugin(
    id = "HtmlExplorerFileType",
    name = "HTML File Type",
    description = "HTML file handling in the explorer perspective",
    image = "ui/images/html.svg")
public class HtmlExplorerFileType extends BaseExplorerFileType<HtmlExplorerFileTypeHandler> {

  public HtmlExplorerFileType() {
    super(
        "HTML File",
        ".html",
        new String[] {"*.html", "*.htm"},
        new String[] {"HTML files"},
        FileTypeCapabilities.getCapabilities(
            IHopFileType.CAPABILITY_SAVE,
            IHopFileType.CAPABILITY_SAVE_AS,
            IHopFileType.CAPABILITY_CLOSE,
            IHopFileType.CAPABILITY_FILE_HISTORY,
            IHopFileType.CAPABILITY_COPY,
            IHopFileType.CAPABILITY_SELECT));
  }

  @Override
  public HtmlExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new HtmlExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public HtmlExplorerFileTypeHandler openFile(HopGui hopGui, String filename, IVariables variables)
      throws HopException {
    String resolvedFilename = variables.resolve(filename);
    if (resolvedFilename.toLowerCase().startsWith("http://")
        || resolvedFilename.toLowerCase().startsWith("https://")) {
      // Create handler without VFS checks for URLs
      // The tab title will be extracted by ExplorerPerspective.getTabDisplayName()
      //
      ExplorerFile explorerFile = new ExplorerFile();
      explorerFile.setName(resolvedFilename); // Will be shortened by tab display logic
      explorerFile.setFilename(resolvedFilename);
      explorerFile.setFileType(this);

      ExplorerPerspective perspective = ExplorerPerspective.getInstance();
      HtmlExplorerFileTypeHandler handler =
          createFileTypeHandler(hopGui, perspective, explorerFile);
      perspective.addFile(handler);
      return handler;
    }
    return super.openFile(hopGui, filename, variables);
  }
}
