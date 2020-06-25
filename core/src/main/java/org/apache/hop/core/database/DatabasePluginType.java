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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;

import java.util.Map;

/**
 * This class represents the transform plugin type.
 *
 * @author matt
 */
public class DatabasePluginType extends BasePluginType<DatabaseMetaPlugin> implements IPluginType<DatabaseMetaPlugin> {
  private static DatabasePluginType pluginType;

  private DatabasePluginType() {
    super( DatabaseMetaPlugin.class, "DATABASE", "Database" );
    populateFolders( "databases" );

    String sharedJdbcDirectory = System.getProperty( Const.HOP_SHARED_JDBC_DIRECTORY );
    if ( StringUtils.isNotEmpty(sharedJdbcDirectory)) {
      extraLibraryFolders.add(sharedJdbcDirectory);
    }
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
  public String getXmlPluginFile() {
    return Const.XML_FILE_HOP_DATABASE_TYPES;
  }

  @Override
  public String getMainTag() {
    return "database-types";
  }

  @Override
  public String getSubTag() {
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
  protected String extractCategory( DatabaseMetaPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( DatabaseMetaPlugin annotation ) {
    return annotation.typeDescription();
  }

  @Override
  protected String extractID( DatabaseMetaPlugin annotation ) {
    return annotation.type();
  }

  @Override
  protected String extractName( DatabaseMetaPlugin annotation ) {
    return annotation.typeDescription();
  }

  @Override
  protected String extractImageFile( DatabaseMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( DatabaseMetaPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( DatabaseMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, DatabaseMetaPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( DatabaseMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( DatabaseMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( DatabaseMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( DatabaseMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( DatabaseMetaPlugin annotation ) {
    return annotation.classLoaderGroup();
  }
}
