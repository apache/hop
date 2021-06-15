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

package org.apache.hop.core.plugins;

import org.apache.hop.core.annotations.PartitionerPlugin;
import org.apache.hop.pipeline.IPartitioner;

import java.util.Map;

/**
 * This is the partitioner plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IPartitioner.class )
@PluginAnnotationType( PartitionerPlugin.class )
public class PartitionerPluginType extends BasePluginType<PartitionerPlugin> {

  private static PartitionerPluginType pluginType;

  private PartitionerPluginType() {
    super( PartitionerPlugin.class, "PARTITIONER", "Partitioner" );
  }

  public static PartitionerPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new PartitionerPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractCategory( PartitionerPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( PartitionerPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( PartitionerPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( PartitionerPlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( PartitionerPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( PartitionerPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( PartitionerPlugin annotation ) {
    return annotation.i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, PartitionerPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( PartitionerPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( PartitionerPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( PartitionerPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( PartitionerPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( PartitionerPlugin annotation ) {
    return annotation.classLoaderGroup();
  }
}
