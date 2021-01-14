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

package org.apache.hop.ui.core.dialog;

import org.eclipse.swt.widgets.FileDialog;

public class NativeFileDialog implements IFileDialog {

  private org.eclipse.swt.widgets.FileDialog fileDialog;

  public NativeFileDialog(FileDialog fileDialog) {
    this.fileDialog = fileDialog;
  }

  @Override
  public void setText(String text) {
    fileDialog.setText(text);
  }

  @Override
  public void setFilterExtensions(String[] filterExtensions) {
    fileDialog.setFilterExtensions(filterExtensions);
  }

  @Override
  public void setFilterNames(String[] filterNames) {
    fileDialog.setFilterNames(filterNames);
  }

  @Override
  public void setFileName(String fileName) {
    fileDialog.setFileName(fileName);
  }

  @Override
  public String getFilterPath() {
    return fileDialog.getFilterPath();
  }

  @Override
  public String getFileName() {
    return fileDialog.getFileName();
  }

  @Override
  public String open() {
    return fileDialog.open();
  }

  @Override
  public void setFilterPath(String filterPath) {
    fileDialog.setFilterPath(filterPath);
  }

  /**
   * Gets fileDialog
   *
   * @return value of fileDialog
   */
  public FileDialog getFileDialog() {
    return fileDialog;
  }

  /** @param fileDialog The fileDialog to set */
  public void setFileDialog(FileDialog fileDialog) {
    this.fileDialog = fileDialog;
  }
}
