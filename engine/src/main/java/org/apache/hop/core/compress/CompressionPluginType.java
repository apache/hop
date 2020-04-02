/*******************************************************************************
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

package org.apache.hop.core.compress;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.IPluginType;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the compression plugin type.
 */
@PluginMainClassType( ICompressionProvider.class )
@PluginAnnotationType( CompressionPlugin.class )
public class CompressionPluginType extends BasePluginType implements IPluginType {
  protected static CompressionPluginType pluginType;

  private CompressionPluginType() {
    super( CompressionPlugin.class, "COMPRESSION", "Compression" );
    populateFolders( "compress" );
  }

  public static CompressionPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new CompressionPluginType();
    }
    return pluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_COMPRESSION_PROVIDERS;
  }

  @Override
  protected String getMainTag() {
    return "compression-providers";
  }

  @Override
  protected String getSubTag() {
    return "compression-provider";
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
    return ( (CompressionPlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).name();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).forumUrl();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (CompressionPlugin) annotation ).classLoaderGroup();
  }
}
