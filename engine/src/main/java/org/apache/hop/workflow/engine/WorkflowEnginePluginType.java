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

package org.apache.hop.workflow.engine;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

@PluginMainClassType( IWorkflowEngine.class )
@PluginAnnotationType( WorkflowEnginePlugin.class )
public class WorkflowEnginePluginType extends BasePluginType<WorkflowEnginePlugin> {

  private WorkflowEnginePluginType() {
    super( WorkflowEnginePlugin.class, "HOP_WORKFLOW_ENGINES", "Hop Workflow Engines" );
  }

  private static WorkflowEnginePluginType pluginType;

  public static WorkflowEnginePluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new WorkflowEnginePluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractCategory( WorkflowEnginePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( WorkflowEnginePlugin annotation ) {
    return ( (WorkflowEnginePlugin) annotation ).description();
  }

  @Override
  protected String extractID( WorkflowEnginePlugin annotation ) {
    return ( (WorkflowEnginePlugin) annotation ).id();
  }

  @Override
  protected String extractName( WorkflowEnginePlugin annotation ) {
    return ( (WorkflowEnginePlugin) annotation ).name();
  }

  @Override
  protected String extractImageFile( WorkflowEnginePlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( WorkflowEnginePlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( WorkflowEnginePlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, WorkflowEnginePlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( WorkflowEnginePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( WorkflowEnginePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( WorkflowEnginePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( WorkflowEnginePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( WorkflowEnginePlugin annotation ) {
    return null;
  }
}
