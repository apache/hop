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
import org.apache.hop.ui.core.dialog.IDirectoryDialog;

public class HopGuiDirectorySelectedExtension {
  public IDirectoryDialog directoryDialog;
  public IVariables variables;
  public String folderName;

  public HopGuiDirectorySelectedExtension( IDirectoryDialog directoryDialog, IVariables variables, String folderName ) {
    this.directoryDialog = directoryDialog;
    this.variables = variables;
    this.folderName = folderName;
  }

  /**
   * Gets directoryDialog
   *
   * @return value of directoryDialog
   */
  public IDirectoryDialog getDirectoryDialog() {
    return directoryDialog;
  }

  /**
   * @param directoryDialog The directoryDialog to set
   */
  public void setDirectoryDialog( IDirectoryDialog directoryDialog ) {
    this.directoryDialog = directoryDialog;
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
   * Gets folderName
   *
   * @return value of folderName
   */
  public String getFolderName() {
    return folderName;
  }

  /**
   * @param folderName The folderName to set
   */
  public void setFolderName( String folderName ) {
    this.folderName = folderName;
  }
}
