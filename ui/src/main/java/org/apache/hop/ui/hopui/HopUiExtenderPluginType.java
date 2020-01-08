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
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeInterface;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@PluginMainClassType( HopUiExtenderPluginInterface.class )
@PluginAnnotationType( HopUiExtenderPlugin.class )
public class HopUiExtenderPluginType extends BasePluginType implements PluginTypeInterface {

  private HopUiExtenderPluginType() {
    super( HopUiExtenderPlugin.class, "SPOONUIEXTENDERPLUGIN", "Spoon UI Extender Plugin" );

    pluginFolders.add( new PluginFolder( "plugins", false, true ) );
  }

  private static HopUiExtenderPluginType pluginType;

  public static HopUiExtenderPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new HopUiExtenderPluginType();
    }
    return pluginType;
  }

  public List<HopUiExtenderPluginInterface> getRelevantExtenders( Class<?> clazz, String uiEvent ) {

    PluginRegistry instance = PluginRegistry.getInstance();
    List<PluginInterface> pluginInterfaces = instance.getPlugins( HopUiExtenderPluginType.class );

    List<HopUiExtenderPluginInterface> relevantPluginInterfaces = new ArrayList<HopUiExtenderPluginInterface>();
    if ( pluginInterfaces != null ) {
      for ( PluginInterface pluginInterface : pluginInterfaces ) {
        try {
          Object loadClass = instance.loadClass( pluginInterface );

          HopUiExtenderPluginInterface hopUiExtenderPluginInterface = (HopUiExtenderPluginInterface) loadClass;

          Set<String> events = hopUiExtenderPluginInterface.respondsTo().get( clazz );
          if ( events != null && events.contains( uiEvent ) ) {
            relevantPluginInterfaces.add( hopUiExtenderPluginInterface );
          }
        } catch ( HopPluginException e ) {
          e.printStackTrace();
        }
      }
    }

    return relevantPluginInterfaces;
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
    return ( (HopUiExtenderPlugin) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (HopUiExtenderPlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (HopUiExtenderPlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (HopUiExtenderPlugin) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (HopUiExtenderPlugin) annotation ).i18nPackageName();
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
    return ( (HopUiExtenderPlugin) annotation ).classLoaderGroup();
  }
}
