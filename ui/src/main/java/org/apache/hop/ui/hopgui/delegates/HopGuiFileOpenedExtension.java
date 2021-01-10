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

package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.dialog.IFileDialog;

public class HopGuiFileOpenedExtension {
  public IFileDialog fileDialog;
  public IVariables variables;
  public String filename;

  public HopGuiFileOpenedExtension( IFileDialog fileDialog, IVariables variables, String filename ) {
    this.fileDialog = fileDialog;
    this.variables = variables;
    this.filename = filename;
  }

  /**
   * Gets fileDialog
   *
   * @return value of fileDialog
   */
  public IFileDialog getFileDialog() {
    return fileDialog;
  }

  /**
   * @param fileDialog The fileDialog to set
   */
  public void setFileDialog( IFileDialog fileDialog ) {
    this.fileDialog = fileDialog;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables( IVariables variables ) {
    this.variables = variables;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }
}
