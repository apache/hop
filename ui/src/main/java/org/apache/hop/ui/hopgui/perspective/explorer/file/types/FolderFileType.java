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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

@HopFileTypePlugin(
  id = "FolderFileType",
  name = "Folder File Type",
  description = "Folder handling in the explorer perspective",
  image = "ui/images/folder.svg")

public class FolderFileType implements IHopFileType {
  @Override
  public String getName() {
    return "folder";
  }

  @Override
  public String getDefaultFileExtension() {
    return "";
  }

  @Override
  public String[] getFilterExtensions() {
    return new String[0];
  }

  @Override
  public String[] getFilterNames() {
    return new String[0];
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
  public IHopFileTypeHandler openFile(
      HopGui hopGui, String filename, IVariables parentVariableSpace) throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace)
      throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  /**
   * See if this is a folder
   *
   * @param filename The filename
   * @param checkContent True if we want to look inside the file content
   * @return
   * @throws HopException
   */
  @Override
  public boolean isHandledBy(String filename, boolean checkContent) throws HopException {
    try {
      return HopVfs.getFileObject(filename).isFolder();
    } catch (Exception e) {
      throw new HopException("Error seeing if file '" + filename + "' is a folder", e);
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
}
