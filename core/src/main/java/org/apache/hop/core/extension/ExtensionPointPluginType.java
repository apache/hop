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

package org.apache.hop.core.extension;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the extension point plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IExtensionPoint.class )
@PluginAnnotationType( ExtensionPoint.class )
public class ExtensionPointPluginType extends BasePluginType<ExtensionPoint> {
  private static ExtensionPointPluginType pluginType;

  private ExtensionPointPluginType() {
    super( ExtensionPoint.class, "EXTENSION_POINT", "Extension point" );
  }

  public static ExtensionPointPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new ExtensionPointPluginType();
    }
    return pluginType;
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( ExtensionPoint annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( ExtensionPoint annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( ExtensionPoint annotation )  {
    return annotation.id();
  }

  @Override
  protected String extractName( ExtensionPoint annotation ) {
    return annotation.extensionPointId();
  }

  @Override
  protected String extractImageFile( ExtensionPoint annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( ExtensionPoint annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( ExtensionPoint annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, ExtensionPoint annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( ExtensionPoint annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( ExtensionPoint annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( ExtensionPoint annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( ExtensionPoint annotation ) {
    return annotation.classLoaderGroup();
  }

  @Override
  protected String extractSuggestion( ExtensionPoint annotation ) {
    return null;
  }
}
