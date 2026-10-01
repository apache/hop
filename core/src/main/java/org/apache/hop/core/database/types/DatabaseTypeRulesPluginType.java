/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.core.database.types;

import java.util.Map;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** The plugin type for externally contributed database type rules. */
@PluginMainClassType(IDatabaseTypeRuleProvider.class)
@PluginAnnotationType(DatabaseTypeRulesPlugin.class)
public class DatabaseTypeRulesPluginType extends BasePluginType<DatabaseTypeRulesPlugin> {

  private static DatabaseTypeRulesPluginType instance;

  private DatabaseTypeRulesPluginType() {
    super(DatabaseTypeRulesPlugin.class, "DATABASE_TYPE_RULES", "Database type rules");
  }

  public static DatabaseTypeRulesPluginType getInstance() {
    if (instance == null) {
      instance = new DatabaseTypeRulesPluginType();
    }
    return instance;
  }

  @Override
  protected String extractCategory(DatabaseTypeRulesPlugin annotation) {
    return null;
  }

  @Override
  protected String extractDesc(DatabaseTypeRulesPlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractID(DatabaseTypeRulesPlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(DatabaseTypeRulesPlugin annotation) {
    return annotation.name().isEmpty() ? annotation.id() : annotation.name();
  }

  @Override
  protected String extractImageFile(DatabaseTypeRulesPlugin annotation) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader(DatabaseTypeRulesPlugin annotation) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected void addExtraClasses(
      Map<Class<?>, String> classMap, Class<?> clazz, DatabaseTypeRulesPlugin annotation) {
    // Nothing extra.
  }

  @Override
  protected String extractDocumentationUrl(DatabaseTypeRulesPlugin annotation) {
    return annotation.documentationUrl();
  }

  @Override
  protected String extractSuggestion(DatabaseTypeRulesPlugin annotation) {
    return null;
  }

  @Override
  protected String extractCasesUrl(DatabaseTypeRulesPlugin annotation) {
    return annotation.casesUrl();
  }

  @Override
  protected String extractForumUrl(DatabaseTypeRulesPlugin annotation) {
    return annotation.forumUrl();
  }

  @Override
  protected String extractClassLoaderGroup(DatabaseTypeRulesPlugin annotation) {
    return annotation.classLoaderGroup();
  }
}
