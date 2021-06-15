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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the row distribution plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IRowDistribution.class )
@PluginAnnotationType( RowDistributionPlugin.class )
public class RowDistributionPluginType extends BasePluginType<RowDistributionPlugin> {
  private static RowDistributionPluginType instance;

  private RowDistributionPluginType() {
    super( RowDistributionPlugin.class, "ROW_DISTRIBUTION", "Row Distribution" );
  }

  public static RowDistributionPluginType getInstance() {
    if ( instance == null ) {
      instance = new RowDistributionPluginType();
    }
    return instance;
  }

  @Override
  protected String extractCategory( RowDistributionPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( RowDistributionPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( RowDistributionPlugin annotation ) {
    return annotation.code();
  }

  @Override
  protected String extractName( RowDistributionPlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( RowDistributionPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( RowDistributionPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( RowDistributionPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, RowDistributionPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( RowDistributionPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( RowDistributionPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( RowDistributionPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( RowDistributionPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( RowDistributionPlugin annotation ) {
    return annotation.classLoaderGroup();
  }
}
