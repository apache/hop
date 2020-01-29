/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginFolder;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.PluginTypeInterface;
import org.apache.hop.ui.hopui.HopUiPlugin;

import java.lang.annotation.Annotation;
import java.util.Map;

@PluginMainClassType( IHopPerspective.class )
@PluginAnnotationType( HopPerspectivePlugin.class )
public class HopPerspectivePluginType extends BasePluginType implements PluginTypeInterface {

  private HopPerspectivePluginType() {
    super( HopUiPlugin.class, "HOP_PERSPECTIVES", "Hop Perspective Plugin" );

    pluginFolders.add( new PluginFolder( "plugins", false, true ) );
  }

  private static HopPerspectivePluginType pluginType;

  public static HopPerspectivePluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new HopPerspectivePluginType();
    }
    return pluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_PERSPECTIVE_PLUGINS;
  }

  @Override
  protected String getMainTag() {
    return "hop-perspective-plugins";
  }

  @Override
  protected String getSubTag() {
    return "hop-perspective-plugin";
  }

  @Override
  protected String getPath() {
    return "./";
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (HopPerspectivePlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (HopPerspectivePlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (HopPerspectivePlugin) annotation ).name();
  }
}
