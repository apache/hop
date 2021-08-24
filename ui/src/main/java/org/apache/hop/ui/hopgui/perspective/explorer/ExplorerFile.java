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

package org.apache.hop.ui.hopgui.perspective.explorer;

import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.eclipse.swt.graphics.Image;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ExplorerFile {

  private String name;
  private Image tabImage;
  private String filename;
  private IExplorerFileType fileType;
  private IExplorerFileTypeHandler fileTypeHandler;
  private boolean changed;
  private List<IContentChangedListener> contentChangedListeners;

  public ExplorerFile() {
    contentChangedListeners = new ArrayList<>();
  }

  public ExplorerFile(
      String name,
      Image tabImage,
      String filename,
      IExplorerFileType fileType,
      IExplorerFileTypeHandler fileTypeHandler) {
    this();
    this.name = name;
    this.tabImage = tabImage;
    this.filename = filename;
    this.fileType = fileType;
    this.fileTypeHandler = fileTypeHandler;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExplorerFile that = (ExplorerFile) o;
    return Objects.equals(filename, that.filename);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filename);
  }

  /**
   * Gets tabName
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets tabImage
   *
   * @return value of tabImage
   */
  public Image getTabImage() {
    return tabImage;
  }

  /** @param tabImage The tabImage to set */
  public void setTabImage(Image tabImage) {
    this.tabImage = tabImage;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /** @param filename The filename to set */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  public IExplorerFileType getFileType() {
    return fileType;
  }

  /** @param fileType The fileType to set */
  public void setFileType(IExplorerFileType fileType) {
    this.fileType = fileType;
  }

  /**
   * Gets fileTypeHandler
   *
   * @return value of fileTypeHandler
   */
  public IExplorerFileTypeHandler getFileTypeHandler() {
    return fileTypeHandler;
  }

  /** @param fileTypeHandler The fileTypeHandler to set */
  public void setFileTypeHandler(IExplorerFileTypeHandler fileTypeHandler) {
    this.fileTypeHandler = fileTypeHandler;
  }

  /**
   * Gets changed
   *
   * @return value of changed
   */
  public boolean isChanged() {
    return changed;
  }

  /** Flag the file as changed */
  public void setChanged() {
    if (!changed) {
      this.changed = true;
      for (IContentChangedListener listener : contentChangedListeners) {
        listener.contentChanged(this);
      }
    }
  }

  public void clearChanged() {
    if (changed) {
      this.changed = false;
      for (IContentChangedListener listener : contentChangedListeners) {
        listener.contentSafe(this);
      }
    }
  }

  /**
   * Gets contentChangedListeners
   *
   * @return value of contentChangedListeners
   */
  public List<IContentChangedListener> getContentChangedListeners() {
    return contentChangedListeners;
  }

  /**
   * Be informed if content changed.
   *
   * @param listener
   */
  public void addContentChangedListener(IContentChangedListener listener) {
    contentChangedListeners.add(listener);
  }
}
