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

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;

import java.util.Map;

/**
 * This plugin type handles the actions.
 *
 * @author matt
 */
@PluginMainClassType( IAction.class )
@PluginAnnotationType( Action.class )
public class ActionPluginType extends BasePluginType<Action> {
  private static final Class<?> PKG = WorkflowMeta.class; // For Translator

  public static final String ID = "ACTION";
  public static final String GENERAL_CATEGORY = BaseMessages.getString( PKG, "ActionCategory.Category.General" );

  private static ActionPluginType pluginType;

  private ActionPluginType() {
    super( Action.class, ID, "Action" );
  }

  protected ActionPluginType( Class<Action> pluginType, String id, String name ) {
    super( pluginType, id, name );
  }

  public static ActionPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new ActionPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractCategory( Action annotation ) {
    return annotation.categoryDescription();
  }

  @Override
  protected String extractDesc( Action annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( Action annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( Action annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( Action annotation ) {
    return annotation.image();
  }

  @Override
  protected boolean extractSeparateClassLoader( Action annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( Action annotation ) {
    return annotation.i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Action annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Action annotation ) {
    return Const.getDocUrl( annotation.documentationUrl() );
  }

  @Override
  protected String extractCasesUrl( Action annotation ) {
    return annotation.casesUrl();
  }

  @Override
  protected String extractForumUrl( Action annotation ) {
    return annotation.forumUrl();
  }

  @Override
  protected String extractSuggestion( Action annotation ) {
    return annotation.suggestion();
  }

  @Override
  protected String extractClassLoaderGroup( Action annotation ) {
    return annotation.classLoaderGroup();
  }

  @Override protected String[] extractKeywords( Action annotation ) {
    return annotation.keywords();
  }
}
