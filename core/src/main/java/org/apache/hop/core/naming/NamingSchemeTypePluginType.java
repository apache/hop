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

package org.apache.hop.core.naming;

import java.util.Map;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

@PluginMainClassType(INamingSchemeType.class)
@PluginAnnotationType(NamingSchemeTypePlugin.class)
public class NamingSchemeTypePluginType extends BasePluginType<NamingSchemeTypePlugin> {

  private static NamingSchemeTypePluginType pluginType;

  private NamingSchemeTypePluginType() {
    super(NamingSchemeTypePlugin.class, "NAMING_SCHEME_TYPE", "NamingSchemeType");
  }

  public static NamingSchemeTypePluginType getInstance() {
    if (pluginType == null) {
      pluginType = new NamingSchemeTypePluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractCategory(NamingSchemeTypePlugin annotation) {
    return "";
  }

  @Override
  protected String extractID(NamingSchemeTypePlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(NamingSchemeTypePlugin annotation) {
    return annotation.name();
  }

  @Override
  protected String extractDesc(NamingSchemeTypePlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractImageFile(NamingSchemeTypePlugin annotation) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader(NamingSchemeTypePlugin annotation) {
    return false;
  }

  @Override
  protected void addExtraClasses(
      Map<Class<?>, String> classMap, Class<?> clazz, NamingSchemeTypePlugin annotation) {
    // Do nothing
  }

  @Override
  protected String extractDocumentationUrl(NamingSchemeTypePlugin annotation) {
    return null;
  }

  @Override
  protected String extractCasesUrl(NamingSchemeTypePlugin annotation) {
    return null;
  }

  @Override
  protected String extractForumUrl(NamingSchemeTypePlugin annotation) {
    return null;
  }

  @Override
  protected String extractSuggestion(NamingSchemeTypePlugin annotation) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup(NamingSchemeTypePlugin annotation) {
    return null;
  }
}
