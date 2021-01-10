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

package org.apache.hop.ui.hopgui;

import org.apache.hop.ui.hopgui.delegates.HopGuiDirectorySelectedExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

public enum HopGuiExtensionPoint {

  HopGuiFileOpenDialog( "Allows you to modify the file open dialog before it's shown. If you want to show your own, set doIt to false", HopGuiFileDialogExtension.class ),
  HopGuiFileOpenedDialog( "Allows you to modify the file open dialog after a file is selected.", HopGuiFileOpenedExtension.class ),
  HopGuiFileSaveDialog( "Allows you to modify the file save dialog before it's shown. If you want to show your own, set doIt to false", HopGuiFileDialogExtension.class ),
  HopGuiNewPipelineTab( "Determine the tab name of a pipeline", HopGuiPipelineGraph.class ),

  HopGuiFileDirectoryDialog( "Called before a DirectoryDialog is presented", HopGuiFileDialogExtension.class ),
  HopGuiDirectorySelected( "Called after a folder is selected in the DirectoryDialog", HopGuiDirectorySelectedExtension.class ),
  ;

  public String id;

  public String description;

  public Class<?> providedClass;


  HopGuiExtensionPoint( String description, Class<?> providedClass ) {
    this.id = name();
    this.description = description;
    this.providedClass = providedClass;
  }
}
