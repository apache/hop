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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.capabilities.FileTypeCapabilities;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileType;

@HopFileTypePlugin(
    id = "CsvExplorerFileType",
    name = "JSON File Type",
    description = "JSON file handling in the explorer perspective",
    image = "textfile.svg")
public class CsvExplorerFileType extends BaseTextExplorerFileType<TextExplorerFileTypeHandler>
    implements IExplorerFileType<TextExplorerFileTypeHandler> {

  public CsvExplorerFileType() {
    super(
        "CSV File",
        ".csv",
        new String[] {"*.csv"},
        new String[] {"CSV files"},
        FileTypeCapabilities.getCapabilities(
            IHopFileType.CAPABILITY_SAVE,
            IHopFileType.CAPABILITY_CLOSE,
            IHopFileType.CAPABILITY_FILE_HISTORY));
  }

  @Override
  public TextExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new TextExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }
}
