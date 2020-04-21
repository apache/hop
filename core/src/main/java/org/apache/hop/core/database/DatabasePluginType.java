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

package org.apache.hop.core.database;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the transform plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IDatabase.class )
@PluginAnnotationType( DatabaseMetaPlugin.class )
public class DatabasePluginType extends BasePluginType implements IPluginType {
  private static DatabasePluginType pluginType;

  private DatabasePluginType() {
    super( DatabaseMetaPlugin.class, "DATABASE", "Database" );
    populateFolders( "databases" );
  }

  public static DatabasePluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new DatabasePluginType();
    }
    return pluginType;
  }

  protected void registerPluginJars() throws HopPluginException {
    super.registerPluginJars();
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_DATABASE_TYPES;
  }

  @Override
  protected String getMainTag() {
    return "database-types";
  }

  @Override
  protected String getSubTag() {
    return "database-type";
  }

  @Override
  protected String getPath() {
    return "./";
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (DatabaseMetaPlugin) annotation ).typeDescription();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (DatabaseMetaPlugin) annotation ).type();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (DatabaseMetaPlugin) annotation ).typeDescription();
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
    return ( (DatabaseMetaPlugin) annotation ).classLoaderGroup();
  }
}
