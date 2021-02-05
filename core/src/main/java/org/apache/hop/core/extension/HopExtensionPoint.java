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

package org.apache.hop.core.extension;

public enum HopExtensionPoint {

  HopGuiInit( "HopGui before the shell is created" ),
  HopGuiStart( "HopGui has started" ),
  HopGuiEnd( "HopGui is about to shut down" ),
  OpenRecent( "A recent file is opened" ),

  PipelinePrepareExecution( "A Pipeline begins to prepare execution" ),
  PipelineStartThreads( "A Pipeline begins to start" ),
  PipelineStart( "A Pipeline has started" ),
  PipelineHeartbeat( "A signal sent at regular intervals to indicate that the Pipeline is still active" ),
  PipelineFinish( "A Pipeline finishes" ),
  PipelineMetaLoaded( "Pipeline metadata was loaded" ),

  PipelinePainterStart( "Draw Pipeline or plugin metadata at the start (below the rest)" ),
  PipelinePainterTransform( "Draw additional information on top of a Pipeline transform icon" ),
  PipelinePainterArrow( "Draw additional information on top of a Pipeline hop (arrow)" ),
  PipelinePainterEnd( "Draw Pipeline or plugin metadata at the end (on top of all the rest)" ),

  PipelineGraphMouseDown( "A mouse down event occurred on the canvas" ),
  PipelineGraphMouseUp( "A mouse up event occurred on the canvas" ),
  PipelineAfterOpen( "A Pipeline file was opened (PipelineMeta)" ),
  PipelineBeforeSave( "A Pipeline file is about to be saved (PipelineMeta)" ),
  PipelineAfterSave( "A Pipeline file was saved (PipelineMeta)" ),
  PipelineBeforeClose( "A Pipeline file is about to be closed" ),
  PipelineAfterClose( "A Pipeline file was closed" ),
  PipelineChanged( "A Pipeline has been changed" ),
  PipelineTransformRightClick( "A right button was clicked on a transform" ),
  PipelineGraphMouseMoved( "The mouse was moved on the canvas" ),
  PipelineGraphMouseDoubleClick( "A left or right button was double-clicked in a Pipeline" ),
  PipelineBeforeDeleteTransforms( "Pipeline transforms about to be deleted" ),

  HopGuiPipelineMetaExecutionStart( "Hop GUI initiates the execution of a pipeline (PipelineMeta)" ),
  HopGuiPipelineExecutionConfiguration( "Right before Hop UI configuration of Pipeline to be executed takes place" ),
  HopGuiPipelineBeforeStart( "Right before the Pipeline is started (Pipeline)" ),

  HopGuiWorkflowBeforeStart( "Right before the workflow is started" ),
  RunConfigurationSelection( "Check when run configuration is selected" ),
  RunConfigurationIsRemote( "Check when run configuration is pointing to a remote server" ),
  HopGuiRunConfiguration( "Send the run configuration" ),

  WorkflowStart( "A workflow starts" ),
  WorkflowFinish( "A workflow finishes" ),
  WorkflowBeforeActionExecution( "Before a action executes" ),
  WorkflowAfterActionExecution( "After a action executes" ),
  WorkflowBeginProcessing( "Start of a workflow at the end of the log table handling" ),

  WorkflowPainterStart( "Draw workflow or plugin metadata at the start (below the rest)" ),
  WorkflowPainterAction( "Draw additional information on top of a action copy icon" ),
  WorkflowPainterArrow( "Draw additional information on top of a workflow hop (arrow)" ),
  WorkflowPainterEnd( "Draw workflow or plugin metadata at the end (on top of all the rest)" ),

  WorkflowGraphMouseDown( "A left or right button was clicked in a workflow" ),
  WorkflowBeforeOpen( "A workflow file is about to be opened" ),
  WorkflowAfterOpen( "A workflow file was opened" ),
  WorkflowBeforeSave( "A workflow file is about to be saved" ),
  WorkflowAfterSave( "A workflow file was saved" ),
  WorkflowBeforeClose( "A workflow file is about to be closed" ),
  WorkflowAfterClose( "A workflow file was closed" ),
  WorkflowChanged( "A workflow has been changed" ),
  WorkflowGraphMouseDoubleClick( "A left or right button was double-clicked in a workflow" ),
  WorkflowDialogShowRetrieveLogTableFields( "Show or retrieve the contents of the fields of a log channel on the log channel composite" ),

  WorkflowMetaLoaded( "Workflow metadata was loaded" ),
  HopGuiWorkflowMetaExecutionStart( "Hop UI initiates the execution of a workflow (WorkflowMeta)" ),
  HopGuiWorkflowExecutionConfiguration( "Right before Hop UI configuration of workflow to be executed takes place" ),

  DatabaseConnected( "A connection to a database was made" ),
  DatabaseDisconnected( "A connection to a database was terminated" ),

  TransformBeforeInitialize( "Right before a transform is about to be initialized" ),
  TransformAfterInitialize( "After a transform is initialized" ),

  TransformBeforeStart( "Right before a transform is about to be started" ),
  TransformFinished( "After a transform has finished" ),

  BeforeCheckTransforms( "Right before a set of transforms is about to be verified." ),
  AfterCheckTransforms( "After a set of transforms has been checked for warnings/errors." ),
  BeforeCheckTransform( "Right before a transform is about to be verified." ),
  AfterCheckTransform( "After a transform has been checked for warnings/errors." ),

  HopServerStartup( "Right after the Hop webserver has started and is fully functional" ),
  HopServerShutdown( "Right before the Hop webserver will shut down" ),

  HopGuiFileOpenDialog( "Allows you to modify the file dialog before it's shown. If you want to show your own, set doIt to false (" ),
  HopGuiNewPipelineTab( "Determine the tab name of a pipeline (HopGuiPipelineGraph)" ),
  HopGuiNewWorkflowTab( "Determine the tab name of a workflow (HopGuiJobGraph)" ),

  HopGuiMetadataObjectCreateBeforeDialog("A new metadata object is created. Before showing the dialog"),
  HopGuiMetadataObjectCreated("A new metadata object is created"),
  HopGuiMetadataObjectUpdated("A metadata object is updated"),
  HopGuiMetadataObjectDeleted("A metadata object is deleted"),

  HopGuiGetSearchablesLocations("Get a list of searchables locations (List<ISearchablesLocation>)"),

  HopGuiPipelineGraphAreaHover("Mouse is hovering over a drawn area in a pipeline graph (HopGuiTooltipExtension)"),
  HopGuiWorkflowGraphAreaHover("Mouse is hovering over a drawn area in a workflow graph (HopGuiTooltipExtension)"),

  HopRunCalculateFilename( "Right after the filename is determined, before it is used in any way" ),
  HopRunStart( "At the start of the HopRun command line, before loading metadata execution" ),
  HopRunEnd( "At the end of the HopRun command line execution" ),

  HopGuiPipelineAfterClose("Called after a pipeline is closed in the Hop GUI (PipelineMeta)"),
  HopGuiWorkflowAfterClose("Called after a workflow is closed in the Hop GUI (WorkflowMeta)"),
  
  GetFieldsExtension( "Get Fields dialog" ),

  HopEnvironmentAfterInit("Called after HopEnvironment.init() was called.  It allows you to add your own plugins and so on at this time."),

  HopGuiProjectAfterEnabled( "Called after a project is enabled in Hop GUI" ),

  HopGuiGetControlSpaceSortOrderPrefix("Gets a prefix to steer the sort order of variables when using CTRL-SPACE.  Defaults range from 900_ to 400_. Set prefixes in Map<String,String>"),
  ;

  public String id;

  public String description;

  public Class<?> providedClass;

  private HopExtensionPoint( String description ) {
    this.id = name();
    this.description = description;
    this.providedClass = Object.class;
  }

  private HopExtensionPoint( String id, String description, Class<?> providedClass ) {
    this.id = id;
    this.description = description;
    this.providedClass = providedClass;
  }
}
