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

package org.apache.hop.core.search;

import org.apache.hop.core.plugins.BasePluginType;

import java.util.Map;

public class SearchableAnalyserPluginType extends BasePluginType<SearchableAnalyserPlugin> {

  private static SearchableAnalyserPluginType pluginType;

  private SearchableAnalyserPluginType() {
    super( SearchableAnalyserPlugin.class, "SEARCH_ANALYSER", "SearchAnalyser" );
  }

  public static SearchableAnalyserPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new SearchableAnalyserPluginType();
    }
    return pluginType;
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( SearchableAnalyserPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractID( SearchableAnalyserPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( SearchableAnalyserPlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractDesc( SearchableAnalyserPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractImageFile( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( SearchableAnalyserPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, SearchableAnalyserPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( SearchableAnalyserPlugin annotation ) {
    return null;
  }
}

