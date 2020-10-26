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

package org.apache.hop.core.logging;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.util.Utils;

import java.util.Map;

/**
 * This class represents the logging plugin type.
 *
 * @author matt
 */
@PluginMainClassType( ILoggingPlugin.class )
@PluginAnnotationType( LoggingPlugin.class )
public class LoggingPluginType extends BasePluginType<LoggingPlugin> implements IPluginType<LoggingPlugin> {

  private static LoggingPluginType loggingPluginType;

  private LoggingPluginType() {
    super( LoggingPlugin.class, "LOGGING", "Logging Plugin" );
    populateFolders( "logging" );
  }

  public static LoggingPluginType getInstance() {
    if ( loggingPluginType == null ) {
      loggingPluginType = new LoggingPluginType();
    }
    return loggingPluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_LOGGING_PLUGINS;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_LOGGING_PLUGINS_FILE;
  }

  @Override
  protected String getMainTag() {
    return "logging-plugins";
  }

  @Override
  protected String getSubTag() {
    return "logging-plugin";
  }

  @Override
  protected String extractCategory( LoggingPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( LoggingPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractID( LoggingPlugin annotation ) {
    String id = annotation.id();
    return id;
  }

  @Override
  protected String extractName( LoggingPlugin annotation ) {
    String name = annotation.name();
    return Utils.isEmpty( name ) ? extractID( annotation ) : name;
  }

  @Override
  protected String extractImageFile( LoggingPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( LoggingPlugin annotation ) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( LoggingPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, LoggingPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( LoggingPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( LoggingPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( LoggingPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( LoggingPlugin annotation ) {
    return annotation.classLoaderGroup();
  }

  @Override
  protected String extractSuggestion( LoggingPlugin annotation ) {
    return null;
  }
}
