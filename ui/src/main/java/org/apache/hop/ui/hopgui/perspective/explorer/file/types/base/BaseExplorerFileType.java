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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.base;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeBase;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.config.ExplorerPerspectiveConfigSingleton;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.MessageBox;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class BaseExplorerFileType<T extends IExplorerFileTypeHandler>
    extends HopFileTypeBase implements IExplorerFileType<T> {

  private String name;
  private String defaultFileExtension;
  private String[] filterExtensions;
  private String[] filterNames;
  private Properties capabilities;

  public BaseExplorerFileType() {}

  public BaseExplorerFileType(
      String name,
      String defaultFileExtension,
      String[] filterExtensions,
      String[] filterNames,
      Properties capabilities) {
    this.name = name;
    this.defaultFileExtension = defaultFileExtension;
    this.filterExtensions = filterExtensions;
    this.filterNames = filterNames;
    this.capabilities = capabilities;
  }

  @Override
  public boolean supportsFile(IHasFilename metaObject) {
    return false;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return Collections.emptyList();
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets defaultFileExtension
   *
   * @return value of defaultFileExtension
   */
  @Override
  public String getDefaultFileExtension() {
    return defaultFileExtension;
  }

  /** @param defaultFileExtension The defaultFileExtension to set */
  public void setDefaultFileExtension(String defaultFileExtension) {
    this.defaultFileExtension = defaultFileExtension;
  }

  /**
   * Gets filterExtensions
   *
   * @return value of filterExtensions
   */
  @Override
  public String[] getFilterExtensions() {
    return filterExtensions;
  }

  /** @param filterExtensions The filterExtensions to set */
  public void setFilterExtensions(String[] filterExtensions) {
    this.filterExtensions = filterExtensions;
  }

  /**
   * Gets filterNames
   *
   * @return value of filterNames
   */
  @Override
  public String[] getFilterNames() {
    return filterNames;
  }

  /** @param filterNames The filterNames to set */
  public void setFilterNames(String[] filterNames) {
    this.filterNames = filterNames;
  }

  /**
   * Gets capabilities
   *
   * @return value of capabilities
   */
  @Override
  public Properties getCapabilities() {
    return capabilities;
  }

  /** @param capabilities The capabilities to set */
  public void setCapabilities(Properties capabilities) {
    this.capabilities = capabilities;
  }

  @Override
  public String getFileTypeImage() {
    return getClass().getAnnotation(HopFileTypePlugin.class).image();
  }

  @Override
  public T openFile(HopGui hopGui, String filename, IVariables parentVariables)
      throws HopException {

    try {
      FileObject fileObject = HopVfs.getFileObject(parentVariables.resolve(filename));
      String name = fileObject.getName().getBaseName();

      // Check the file size before opening.
      // We don't want to accidentally load 25 TB in memory.
      //
      long fileSize = fileObject.getContent().getSize();
      long sizeMb = fileSize / (1024 * 1024);

      // What is the maximum size?
      //
      String maxSizeOption = ExplorerPerspectiveConfigSingleton.getConfig().getFileLoadingMaxSize();
      long maxSizeMb = Const.toLong(hopGui.getVariables().resolve(maxSizeOption), 16);

      if (sizeMb > maxSizeMb) {
        MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("Open large file?");
        box.setMessage(
            name
                + Const.CR
                + Const.CR
                + "The file to open is "
                + sizeMb
                + "MB in size. "
                + "This is larger than the configured maximum of "
                + maxSizeMb
                + "MB.  There might be a danger of running out of memory. "
                + Const.CR
                + Const.CR
                + "Are you sure you still want to open this file?");
        int answer = box.open();
        if ((answer & SWT.YES) == 0) {
          return null;
        }
      }

      // Open the file in the explorer perspective
      //
      ExplorerPerspective perspective = ExplorerPerspective.getInstance();

      ExplorerFile explorerFile = new ExplorerFile();
      explorerFile.setName(Const.NVL(name, ""));
      explorerFile.setFilename(filename);
      explorerFile.setFileType(this);
      explorerFile.setTabImage(perspective.getFileTypeImage(this));

      T fileTypeHandler = createFileTypeHandler(hopGui, perspective, explorerFile);
      explorerFile.setFileTypeHandler(fileTypeHandler);

      perspective.addFile(explorerFile, fileTypeHandler);

      return fileTypeHandler;
    } catch (Exception e) {
      throw new HopException(
          "Error opening file '" + filename + "' in a new tab in the Explorer perspective", e);
    }
  }

  @Override
  public abstract T createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file);
}
