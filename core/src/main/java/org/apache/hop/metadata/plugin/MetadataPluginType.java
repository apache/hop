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

package org.apache.hop.metadata.plugin;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;

import java.util.Map;

/**
 * This class defines how Hop Metadata plugins are loaded
 */
@PluginMainClassType( IHopMetadata.class )
@PluginAnnotationType( HopMetadata.class )
public class MetadataPluginType extends BasePluginType<HopMetadata> {
  private static MetadataPluginType pluginType;

  private MetadataPluginType() {
    super( HopMetadata.class, "METADATA", "Metadata" );
  }

  public static MetadataPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new MetadataPluginType();
    }
    return pluginType;
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( HopMetadata annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( HopMetadata annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( HopMetadata annotation ) {
    return Const.NVL( annotation.key(), annotation.name() );
  }

  @Override
  protected String extractName( HopMetadata annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( HopMetadata annotation ) {
    return annotation.image();
  }

  @Override
  protected boolean extractSeparateClassLoader( HopMetadata annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( HopMetadata annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, HopMetadata annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( HopMetadata annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( HopMetadata annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( HopMetadata annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( HopMetadata annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( HopMetadata annotation ) {
    return null;
  }
}
