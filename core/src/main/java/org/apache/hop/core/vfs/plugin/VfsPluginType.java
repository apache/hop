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

package org.apache.hop.core.vfs.plugin;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the transform plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IVfs.class )
@PluginAnnotationType( VfsPlugin.class )
public class VfsPluginType extends BasePluginType<VfsPlugin> {
  private static VfsPluginType pluginType;

  private VfsPluginType() {
    super( VfsPlugin.class, "VFS", "VFS" );
  }

  public static VfsPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new VfsPluginType();
    }
    return pluginType;
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( VfsPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( VfsPlugin annotation ) {
    return annotation.typeDescription();
  }

  @Override
  protected String extractID( VfsPlugin annotation ) {
    return annotation.type();
  }

  @Override
  protected String extractName( VfsPlugin annotation ) {
    return annotation.typeDescription();
  }

  @Override
  protected String extractImageFile( VfsPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( VfsPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( VfsPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, VfsPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( VfsPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( VfsPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( VfsPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( VfsPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( VfsPlugin annotation ) {
    return null;
  }
}
