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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.noext;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
    id = "NoExtensionExplorerFileType",
    name = "Text files without extensions",
    description = "No extension files handling in the explorer perspective",
    image = "ui/images/script-active.svg")
public class NoExtensionExplorerFileType
    extends BaseTextExplorerFileType<NoExtensionExplorerFileTypeHandler>
    implements IExplorerFileType<NoExtensionExplorerFileTypeHandler> {

  public NoExtensionExplorerFileType() {
    super(
        "Text files without extensions",
        "",
        new String[] {
          "config",
          ".gitignore",
          ".profile",
          ".bashrc",
          ".gitconfig",
          "Dockerfile",
          "Jenkinsfile",
          "README",
          "READ.me"
        },
        new String[] {
          "Config file",
          "Git ignore file",
          "Profile config",
          "Bash startup script",
          "Git config file",
          "Docker file",
          "Jenkins file",
          "README file",
          "README file"
        },
        FileTypeCapabilities.getCapabilities(IHopFileType.CAPABILITY_SAVE));
  }

  @Override
  public boolean isHandledBy(String filename, boolean checkContent) throws HopException {
    FileObject fileObject = HopVfs.getFileObject(filename);
    String baseName = fileObject.getName().getBaseName();

    for (String extension : getFilterExtensions()) {
      if (extension.equals(baseName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public NoExtensionExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new NoExtensionExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    // Not implemented
    return new EmptyHopFileTypeHandler();
  }
}
