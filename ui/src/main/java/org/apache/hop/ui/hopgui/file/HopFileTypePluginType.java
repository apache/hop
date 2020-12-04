/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopgui.file;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginFolder;
import org.apache.hop.core.plugins.PluginMainClassType;

@PluginMainClassType( IHopFileType.class )
@PluginAnnotationType( HopFileTypePlugin.class )
public class HopFileTypePluginType extends BasePluginType<HopFileTypePlugin> implements IPluginType<HopFileTypePlugin> {

  private HopFileTypePluginType() {
    super( HopFileTypePlugin.class, "HOP_FILE_TYPES", "Hop File Type" );

    pluginFolders.add( new PluginFolder( "plugins", false, true ) );
  }

  private static HopFileTypePluginType pluginType;

  public static HopFileTypePluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new HopFileTypePluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractDesc( HopFileTypePlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( HopFileTypePlugin annotation ) {
    return  annotation.id();
  }

  @Override
  protected String extractName( HopFileTypePlugin annotation ) {
    return  annotation.name();
  }
}
