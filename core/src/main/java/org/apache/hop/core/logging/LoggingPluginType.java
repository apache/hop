/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.logging;

import org.apache.hop.core.plugins.BasePluginType;
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
public class LoggingPluginType extends BasePluginType<LoggingPlugin> {

  private static LoggingPluginType loggingPluginType;

  private LoggingPluginType() {
    super( LoggingPlugin.class, "LOGGING", "Logging Plugin" );
  }

  public static LoggingPluginType getInstance() {
    if ( loggingPluginType == null ) {
      loggingPluginType = new LoggingPluginType();
    }
    return loggingPluginType;
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
