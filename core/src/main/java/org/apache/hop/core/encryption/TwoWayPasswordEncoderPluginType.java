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

package org.apache.hop.core.encryption;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the value meta plugin type.
 *
 * @author matt
 */

@PluginMainClassType( ITwoWayPasswordEncoder.class )
@PluginAnnotationType( TwoWayPasswordEncoderPlugin.class )
public class TwoWayPasswordEncoderPluginType extends BasePluginType<TwoWayPasswordEncoderPlugin> {

  private static TwoWayPasswordEncoderPluginType twoWayPasswordEncoderPluginType;

  private TwoWayPasswordEncoderPluginType() {
    super( TwoWayPasswordEncoderPlugin.class, "TWOWAYPASSWORDENCODERPLUGIN", "TwoWayPasswordEncoder" );
  }

  public static TwoWayPasswordEncoderPluginType getInstance() {
    if ( twoWayPasswordEncoderPluginType == null ) {
      twoWayPasswordEncoderPluginType = new TwoWayPasswordEncoderPluginType();
    }
    return twoWayPasswordEncoderPluginType;
  }

  @Override
  protected String extractCategory( TwoWayPasswordEncoderPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( TwoWayPasswordEncoderPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( TwoWayPasswordEncoderPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, TwoWayPasswordEncoderPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.documentationUrl();
  }

  @Override
  protected String extractCasesUrl( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.casesUrl();
  }

  @Override
  protected String extractForumUrl( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.forumUrl();
  }

  @Override
  protected String extractSuggestion( TwoWayPasswordEncoderPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( TwoWayPasswordEncoderPlugin annotation ) {
    return annotation.classLoaderGroup();
  }
}
