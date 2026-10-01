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
 *
 */

package org.apache.hop.pipeline.transforms.types;

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

/**
 * The rendered preview of a Markdown file. This type never matches a file on disk: it only exists
 * so that the preview of a .md file can live in its own tab, opened from the content editor toolbar
 * of {@link MarkDownExplorerFileTypeHandler}.
 */
@HopFileTypePlugin(
    id = "MarkDownPreviewExplorerFileType",
    name = "MarkDown Preview File Type",
    description = "Rendered MarkDown preview in the explorer perspective",
    image = "markdown.svg")
public class MarkDownPreviewExplorerFileType
    extends BaseExplorerFileType<MarkDownPreviewExplorerFileTypeHandler> {

  public MarkDownPreviewExplorerFileType() {
    super(
        "MarkDown Preview",
        "",
        new String[] {},
        new String[] {},
        FileTypeCapabilities.getCapabilities(IHopFileType.CAPABILITY_CLOSE));
  }

  @Override
  public MarkDownPreviewExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new MarkDownPreviewExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace) {
    return new EmptyHopFileTypeHandler();
  }
}
