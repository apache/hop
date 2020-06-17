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

package org.apache.hop.core.config.plugin;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the transform plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IConfigOptions.class )
@PluginAnnotationType( ConfigPlugin.class )
public class ConfigPluginType extends BasePluginType<ConfigPlugin> implements IPluginType<ConfigPlugin> {
  private static ConfigPluginType pluginType;

  private ConfigPluginType() {
    super( ConfigPlugin.class, "CONFIG", "Configuration" );
    populateFolders( "configs" );
  }

  public static ConfigPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new ConfigPluginType();
    }
    return pluginType;
  }

  protected void registerPluginJars() throws HopPluginException {
    super.registerPluginJars();
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_CONFIG_PLUGINS;
  }

  @Override
  protected String getMainTag() {
    return "config-plugins";
  }

  @Override
  protected String getSubTag() {
    return "config-plugin";
  }

  @Override
  protected String getPath() {
    return "./";
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( ConfigPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( ConfigPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( ConfigPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( ConfigPlugin annotation ) {
    return annotation.guiPluginId();
  }

  @Override
  protected String extractImageFile( ConfigPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( ConfigPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( ConfigPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, ConfigPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( ConfigPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( ConfigPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( ConfigPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( ConfigPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( ConfigPlugin annotation ) {
    return null;
  }
}
