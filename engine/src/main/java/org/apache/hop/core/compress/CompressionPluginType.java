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

package org.apache.hop.core.compress;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the compression plugin type.
 */
@PluginMainClassType( ICompressionProvider.class )
@PluginAnnotationType( CompressionPlugin.class )
public class CompressionPluginType extends BasePluginType<CompressionPlugin> {
  protected static CompressionPluginType pluginType;

  private CompressionPluginType() {
    super( CompressionPlugin.class, "COMPRESSION", "Compression" );
  }

  public static CompressionPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new CompressionPluginType();
    }
    return pluginType;
  }
  
  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( CompressionPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).description();
  }

  @Override
  protected String extractID( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).id();
  }

  @Override
  protected String extractName( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).name();
  }

  @Override
  protected boolean extractSeparateClassLoader( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, CompressionPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).forumUrl();
  }

  @Override
  protected String extractImageFile( CompressionPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( CompressionPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( CompressionPlugin annotation ) {
    return ( (CompressionPlugin) annotation ).classLoaderGroup();
  }
}
