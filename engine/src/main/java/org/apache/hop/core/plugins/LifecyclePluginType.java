/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.core.plugins;

import org.apache.hop.core.annotations.LifecyclePlugin;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.IGUIOption;
import org.apache.hop.core.lifecycle.ILifecycleListener;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the lifecycle plugin type.
 *
 * @author matt
 */
@PluginMainClassType( ILifecycleListener.class )
@PluginExtraClassTypes( classTypes = { IGUIOption.class } )
@PluginAnnotationType( LifecyclePlugin.class )
public class LifecyclePluginType extends BasePluginType implements IPluginType {

  private static LifecyclePluginType pluginType;

  private LifecyclePluginType() {
    super( LifecyclePlugin.class, "LIFECYCLE LISTENERS", "Lifecycle listener plugin type" );
    populateFolders( "repositories" );
  }

  public static LifecyclePluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new LifecyclePluginType();
    }
    return pluginType;
  }

  /**
   * Scan & register internal transform plugins
   */
  protected void registerNatives() throws HopPluginException {
    // Up until now, we have no natives.
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return "";
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (LifecyclePlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (LifecyclePlugin) annotation ).name();
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
    return null;
  }

  /**
   * Extract extra classes information from a plugin annotation.
   *
   * @param classMap
   * @param annotation
   */
  public void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
    // LifecyclePlugin plugin = (LifecyclePlugin) annotation;
    classMap.put( IGUIOption.class, clazz.getName() );
    classMap.put( ILifecycleListener.class, clazz.getName() );
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
    return ( (LifecyclePlugin) annotation ).classLoaderGroup();
  }
}
