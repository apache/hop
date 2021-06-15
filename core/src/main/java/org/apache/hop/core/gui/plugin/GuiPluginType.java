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

package org.apache.hop.core.gui.plugin;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;

import java.util.Map;

/**
 * This class represents a plugin type for GUI elements like menus and toolbars.
 *
 * @author matt
 */
@PluginAnnotationType( GuiPlugin.class )
public class GuiPluginType extends BasePluginType<GuiPlugin> {
  private static GuiPluginType pluginType;

  private GuiPluginType() {
    super( GuiPlugin.class, "GUI", "GUI" );
  }

  public static GuiPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new GuiPluginType();
    }
    return pluginType;
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( GuiPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( GuiPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( GuiPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( GuiPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractImageFile( GuiPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( GuiPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( GuiPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, GuiPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( GuiPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( GuiPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( GuiPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( GuiPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( GuiPlugin annotation ) {
    return null;
  }
}
