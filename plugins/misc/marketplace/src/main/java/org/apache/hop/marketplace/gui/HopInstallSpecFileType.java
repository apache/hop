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

package org.apache.hop.marketplace.gui;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.marketplace.env.HopInstallSpecFiles;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeBase;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.capabilities.FileTypeCapabilities;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileType;

@HopFileTypePlugin(
    id = "HopInstallSpecFileType",
    name = "Hop install spec",
    description = "Hop install spec (hop-env.yaml) editor",
    image = "ui/images/marketplace.svg")
public class HopInstallSpecFileType extends BaseExplorerFileType<HopInstallSpecFileTypeHandler> {

  public HopInstallSpecFileType() {
    super(
        "Hop install spec",
        ".yaml",
        HopInstallSpecFiles.WELL_KNOWN_NAMES.toArray(new String[0]),
        new String[] {
          "Hop install spec", "Hop install spec", "Hop install spec", "Hop install spec"
        },
        FileTypeCapabilities.getCapabilities(
            IHopFileType.CAPABILITY_SAVE,
            IHopFileType.CAPABILITY_SAVE_AS,
            IHopFileType.CAPABILITY_CLOSE,
            IHopFileType.CAPABILITY_FILE_HISTORY));
  }

  @Override
  public boolean isHandledBy(String filename, boolean checkContent) throws HopException {
    return HopInstallSpecFiles.isWellKnown(filename);
  }

  @Override
  public HopInstallSpecFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new HopInstallSpecFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  /**
   * True when {@code filename} is a YAML/JSON file that can be opened as an install spec, including
   * custom names such as {@code deploy/plugins.yaml}.
   */
  public static boolean isYamlOrJson(String filename) {
    String ext = HopFileTypeBase.extractExtension(filename);
    return "yaml".equals(ext) || "yml".equals(ext) || "json".equals(ext);
  }
}
