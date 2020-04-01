/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui;

import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenExtension;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

public enum HopGuiExtensionPoint {

  HopGuiFileOpenDialog( "HopGuiFileOpenDialog", "Allows you to modify the file dialog before it's shown. If you want to show your own, set doIt to false", HopGuiFileOpenExtension.class ),
  HopGuiNewPipelineTab( "HopGuiPipelineTab", "Determine the tab name of a pipeline", HopGuiPipelineGraph.class ),
  ;

  public String id;

  public String description;

  public Class<?> providedClass;

  private HopGuiExtensionPoint( String id, String description ) {
    this.id = id;
    this.description = description;
    this.providedClass = Object.class;
  }

  private HopGuiExtensionPoint( String id, String description, Class<?> providedClass ) {
    this.id = id;
    this.description = description;
    this.providedClass = providedClass;
  }
}
