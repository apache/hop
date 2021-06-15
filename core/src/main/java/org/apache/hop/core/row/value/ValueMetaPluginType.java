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

package org.apache.hop.core.row.value;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.row.IValueMeta;

import java.util.Map;

/**
 * This class represents the value meta plugin type.
 *
 * @author matt
 */

@PluginMainClassType( IValueMeta.class )
@PluginAnnotationType( ValueMetaPlugin.class )
public class ValueMetaPluginType extends BasePluginType<ValueMetaPlugin> {

  private static ValueMetaPluginType valueMetaPluginType;

  private ValueMetaPluginType() {
    super( ValueMetaPlugin.class, "VALUEMETA", "ValueMeta" );
  }

  public static ValueMetaPluginType getInstance() {
    if ( valueMetaPluginType == null ) {
      valueMetaPluginType = new ValueMetaPluginType();
    }
    return valueMetaPluginType;
  }

  @Override
  protected String extractCategory( ValueMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( ValueMetaPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( ValueMetaPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( ValueMetaPlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( ValueMetaPlugin annotation ) {
    return annotation.image();
  }

  @Override
  protected boolean extractSeparateClassLoader( ValueMetaPlugin annotation ) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( ValueMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, ValueMetaPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( ValueMetaPlugin annotation ) {
    return annotation.documentationUrl();
  }

  @Override
  protected String extractSuggestion( ValueMetaPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( ValueMetaPlugin annotation ) {
    return annotation.casesUrl();
  }

  @Override
  protected String extractForumUrl( ValueMetaPlugin annotation ) {
    return annotation.forumUrl();
  }

  @Override
  protected String extractClassLoaderGroup( ValueMetaPlugin annotation ) {
    return annotation.classLoaderGroup();
  }
}
