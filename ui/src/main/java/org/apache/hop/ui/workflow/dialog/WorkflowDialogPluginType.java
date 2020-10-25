/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.workflow.dialog;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This plugin allows you to capture additional information concerning actions.
 *
 * @author matt
 */
@PluginMainClassType( IWorkflowDialogPlugin.class )
@PluginAnnotationType( WorkflowDialogPlugin.class )
public class WorkflowDialogPluginType extends BasePluginType<WorkflowDialogPlugin> implements IPluginType<WorkflowDialogPlugin> {
  // private static final Class<?> PKG = WorkflowDialogPluginType.class; // for i18n purposes, needed by Translator!!

  private static WorkflowDialogPluginType pluginType;

  private WorkflowDialogPluginType() {
    super( WorkflowDialogPlugin.class, "JOBDIALOG", "Workflow dialog" );
    populateFolders( "jobdialog" );
  }

  public static WorkflowDialogPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new WorkflowDialogPluginType();
    }
    return pluginType;
  }

  /**
   * Scan & register internal transform plugins
   */
  protected void registerNatives() throws HopPluginException {
    // No native plugins
  }

  @Override
  protected String extractCategory( WorkflowDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( WorkflowDialogPlugin annotation ) {
    return ( (WorkflowDialogPlugin) annotation ).description();
  }

  @Override
  protected String extractID( WorkflowDialogPlugin annotation ) {
    return ( (WorkflowDialogPlugin) annotation ).id();
  }

  @Override
  protected String extractName( WorkflowDialogPlugin annotation ) {
    return ( (WorkflowDialogPlugin) annotation ).name();
  }

  @Override
  protected String extractImageFile( WorkflowDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( WorkflowDialogPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( WorkflowDialogPlugin annotation ) {
    return ( (WorkflowDialogPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, WorkflowDialogPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( WorkflowDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( WorkflowDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( WorkflowDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( WorkflowDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( WorkflowDialogPlugin annotation ) {
    return ( (WorkflowDialogPlugin) annotation ).classLoaderGroup();
  }
}
