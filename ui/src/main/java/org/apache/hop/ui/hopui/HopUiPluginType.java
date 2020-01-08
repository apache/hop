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

package org.apache.hop.ui.hopui;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginFolder;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.PluginTypeInterface;

import java.lang.annotation.Annotation;
import java.util.Map;

@PluginMainClassType( HopUiPlugin.class )
@PluginAnnotationType( HopUiPlugin.class )
public class HopUiPluginType extends BasePluginType implements PluginTypeInterface {

  private HopUiPluginType() {
    super( HopUiPlugin.class, "SPOONPLUGIN", "Spoon Plugin" );

    pluginFolders.add( new PluginFolder( "plugins", false, true ) );
  }

  private static HopUiPluginType pluginType;

  public static HopUiPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new HopUiPluginType();
    }
    return pluginType;
  }

  @Override
  protected void registerNatives() throws HopPluginException {
    // TODO Auto-generated method stub
  }

  @Override
  protected void registerXmlPlugins() throws HopPluginException {
    // TODO Auto-generated method stub
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return ( (HopUiPlugin) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (HopUiPlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (HopUiPlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (HopUiPlugin) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (HopUiPlugin) annotation ).image();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (HopUiPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (HopUiPlugin) annotation ).classLoaderGroup();
  }
}
