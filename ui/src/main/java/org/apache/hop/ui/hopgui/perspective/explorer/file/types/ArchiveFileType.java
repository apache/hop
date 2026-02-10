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
 *
 */

package org.apache.hop.ui.hopgui.perspective.explorer.file.types;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;

@HopFileTypePlugin(
    id = "ArchiveExplorerFileType",
    name = "Archive File Type",
    description = "Archive file handling in the explorer perspective",
    image = "ui/images/zipfile.svg")
public class ArchiveFileType implements IHopFileType {

  private static final String[] EXTENSIONS = new String[] {"*.zip", "*.7z", "*.gz", "*.rar"};
  private static final String[] FILTER_EXTENSIONS = new String[] {"*.zip;*.7z;*.gz;*.rar"};
  private static final String[] FILTER_NAMES = new String[] {"Archive files"};

  @Override
  public String getName() {
    return "archive";
  }

  @Override
  public String getDefaultFileExtension() {
    return "zip";
  }

  @Override
  public String[] getFilterExtensions() {
    return FILTER_EXTENSIONS;
  }

  @Override
  public String[] getFilterNames() {
    return FILTER_NAMES;
  }

  @Override
  public Properties getCapabilities() {
    return new Properties();
  }

  @Override
  public boolean hasCapability(String capability) {
    return false;
  }

  @Override
  public IHopFileTypeHandler openFile(HopGui hopGui, String filename, IVariables variables)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables variables) throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public boolean isHandledBy(String filename, boolean checkContent) throws HopException {
    try {
      FileObject fileObject = HopVfs.getFileObject(filename);
      FileName fileName = fileObject.getName();
      String fileExtension = fileName.getExtension().toLowerCase();

      // No extension
      if (Utils.isEmpty(fileExtension)) return false;

      // Verify the extension
      //
      for (String typeExtension : EXTENSIONS) {
        if (typeExtension.toLowerCase().endsWith(fileExtension)) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      throw new HopException(
          "Unable to verify file handling of file '" + filename + "' by extension", e);
    }
  }

  @Override
  public boolean supportsFile(IHasFilename metaObject) {
    return false;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return Collections.emptyList();
  }

  @Override
  public String getFileTypeImage() {
    return getClass().getAnnotation(HopFileTypePlugin.class).image();
  }

  @Override
  public boolean supportsOpening() {
    return false;
  }
}
