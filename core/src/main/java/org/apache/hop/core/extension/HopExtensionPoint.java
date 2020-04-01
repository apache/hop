/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.extension;

public enum HopExtensionPoint {

  HopUiStart( "HopUiStart", "HopUi has started" ),
  OpenRecent( "OpenRecent", "A recent file is opened" ),

  PipelinePrepareExecution( "PipelinePrepareExecution", "A Pipeline begins to prepare execution" ),
  PipelineStartThreads( "PipelineStartThreads", "A Pipeline begins to start" ),
  PipelineStart( "PipelineStart", "A Pipeline has started" ),
  PipelineHeartbeat( "PipelineHeartbeat", "A signal sent at regular intervals to indicate that the Pipeline is still active" ),
  PipelineFinish( "PipelineFinish", "A Pipeline finishes" ),
  PipelineMetaLoaded( "PipelineMetaLoaded", "Pipeline metadata was loaded" ),
  PipelinePainterArrow( "PipelinePainterArrow", "Draw additional information on top of a Pipeline hop (arrow)" ),
  PipelinePainterStep( "PipelinePainterStep", "Draw additional information on top of a Pipeline step icon" ),
  PipelinePainterFlyout( "PipelinePainterFlyout", "Draw step flyout when step is clicked" ),
  PipelinePainterFlyoutTooltip( "PipelinePainterFlyoutTooltip", "Draw the flyout tooltips" ),
  PipelinePainterStart( "PipelinePainterStart", "Draw Pipeline or plugin metadata at the start (below the rest)" ),
  PipelinePainterEnd( "PipelinePainterEnd", "Draw Pipeline or plugin metadata at the end (on top of all the rest)" ),
  PipelineGraphMouseDown( "PipelineGraphMouseDown", "A mouse down event occurred on the canvas" ),
  PipelineGraphMouseUp( "PipelineGraphMouseUp", "A mouse up event occurred on the canvas" ),
  PipelineBeforeOpen( "PipelineBeforeOpen", "A Pipeline file is about to be opened" ),
  PipelineAfterOpen( "PipelineAfterOpen", "A Pipeline file was opened" ),
  PipelineBeforeSave( "PipelineBeforeSave", "A Pipeline file is about to be saved" ),
  PipelineAfterSave( "PipelineAfterSave", "A Pipeline file was saved" ),
  PipelineBeforeClose( "PipelineBeforeClose", "A Pipeline file is about to be closed" ),
  PipelineAfterClose( "PipelineAfterClose", "A Pipeline file was closed" ),
  PipelineChanged( "PipelineChanged", "A Pipeline has been changed" ),
  PipelineStepRightClick( "PipelineStepRightClick", "A right button was clicked on a step" ),
  PipelineGraphMouseMoved( "PipelineGraphMouseMoved", "The mouse was moved on the canvas" ),
  PipelineGraphMouseDoubleClick( "PipelineGraphMouseDoubleClick", "A left or right button was double-clicked in a Pipeline" ),
  PipelineBeforeDeleteSteps( "PipelineBeforeDeleteSteps", "Pipeline steps about to be deleted" ),
  HopGuiPipelineMetaExecutionStart( "HopGuiPipelineMetaExecutionStart", "Hop GUI initiates the execution of a pipeline (PipelineMeta)" ),
  HopUiPipelineExecutionConfiguration( "HopUiPipelineExecutionConfiguration", "Right before Hop UI configuration of Pipeline to be executed takes place" ),
  HopUiPipelineBeforeStart( "HopUiPipelineBeforeStart", "Right before the Pipeline is started" ),
  HopUiJobBeforeStart( "HopUiJobBeforeStart", "Right before the job is started" ),
  RunConfigurationSelection( "RunConfigurationSelection", "Check when run configuration is selected" ),
  RunConfigurationIsRemote( "RunConfigurationIsRemote", "Check when run configuration is pointing to a remote server" ),
  HopUiRunConfiguration( "HopUiRunConfiguration", "Send the run configuration" ),
  JobStart( "JobStart", "A job starts" ),
  JobHeartbeat( "JobHeartbeat", "A signal sent at regular intervals to indicate that the job is still active" ),
  JobFinish( "JobFinish", "A job finishes" ),
  JobBeforeJobEntryExecution( "JobBeforeJobEntryExecution", "Before a job entry executes" ),
  JobAfterJobEntryExecution( "JobAfterJobEntryExecution", "After a job entry executes" ),
  JobBeginProcessing( "JobBeginProcessing", "Start of a job at the end of the log table handling" ),
  JobPainterArrow( "JobPainterArrow", "Draw additional information on top of a job hop (arrow)" ),
  JobPainterJobEntry( "PipelinePainterJobEntry", "Draw additional information on top of a job entry copy icon" ),
  JobPainterStart( "JobPainterStart", "Draw job or plugin metadata at the start (below the rest)" ),
  JobPainterEnd( "JobPainterEnd", "Draw job or plugin metadata at the end (on top of all the rest)" ),
  JobGraphMouseDown( "JobGraphMouseDown", "A left or right button was clicked in a job" ),
  JobBeforeOpen( "JobBeforeOpen", "A job file is about to be opened" ),
  JobAfterOpen( "JobAfterOpen", "A job file was opened" ),
  JobBeforeSave( "JobBeforeSave", "A job file is about to be saved" ),
  JobAfterSave( "JobAfterSave", "A job file was saved" ),
  JobBeforeClose( "JobBeforeClose", "A job file is about to be closed" ),
  JobAfterClose( "JobAfterClose", "A job file was closed" ),
  JobChanged( "JobChanged", "A job has been changed" ),
  JobGraphMouseDoubleClick( "JobGraphMouseDoubleClick", "A left or right button was double-clicked in a job" ),
  JobGraphJobEntrySetMenu( "JobGraphJobEntrySetMenu", "Manipulate the menu on right click on a job entry" ),
  JobDialogShowRetrieveLogTableFields( "JobDialogShowRetrieveLogTableFields", "Show or retrieve the contents of the fields of a log channel on the log channel composite" ),
  JobEntryPipelineSave( "JobEntryPipelineSave", "Job entry Pipeline is saved" ),

  JobMetaLoaded( "JobMetaLoaded", "Job metadata was loaded" ),
  HopUiJobMetaExecutionStart( "HopUiJobMetaExecutionStart", "Hop UI initiates the execution of a job (JobMeta)" ),
  HopUiJobExecutionConfiguration( "HopUiJobExecutionConfiguration", "Right before Hop UI configuration of job to be executed takes place" ),

  DatabaseConnected( "DatabaseConnected", "A connection to a database was made" ),
  DatabaseDisconnected( "DatabaseDisconnected", "A connection to a database was terminated" ),

  StepBeforeInitialize( "StepBeforeInitialize", "Right before a step is about to be initialized" ),
  StepAfterInitialize( "StepAfterInitialize", "After a step is initialized" ),

  StepBeforeStart( "StepBeforeStart", "Right before a step is about to be started" ),
  StepFinished( "StepFinished", "After a step has finished" ),

  BeforeCheckSteps( "BeforeCheckSteps", "Right before a set of steps is about to be verified." ),
  AfterCheckSteps( "AfterCheckSteps", "After a set of steps has been checked for warnings/errors." ),
  BeforeCheckStep( "BeforeCheckStep", "Right before a step is about to be verified." ),
  AfterCheckStep( "AfterCheckStep", "After a step has been checked for warnings/errors." ),

  HopServerStartup( "HopServerStartup", "Right after the Carte webserver has started and is fully functional" ),
  HopServerShutdown( "HopServerShutdown", "Right before the Carte webserver will shut down" ),

  HopUiViewTreeExtension( "HopUiViewTreeExtension", "View tree Hop UI extension" ),
  HopUiPopupMenuExtension( "HopUiPopupMenuExtension", "Pop up menu extension for the view tree" ),
  HopUiTreeDelegateExtension( "HopUiTreeDelegateExtension", "During the HopUiTreeDelegate execution" ),
  HopUiBrowserFunction( "HopUiBrowserFunction", "Generic browser function handler" ),
  GetFieldsExtension( "GetFieldsExtension", "Get Fields dialog" ),

  OpenMapping( "OpenMapping", "Trigger when opening a mapping from PipelineGraph" ),

  PipelineCreateNew( "PipelineCreateNew", "Create a New Empty Pipeline in the Hop UI" ),

  HopGuiFileOpenDialog( "HopGuiFileOpenDialog", "Allows you to modify the file dialog before it's shown. If you want to show your own, set doIt to false (" ),
  HopGuiNewPipelineTab( "HopGuiNewPipelineTab", "Determine the tab name of a Pipeline (HopGuiPipelineGraph)" ),
  HopGuiNewJobTab( "HopGuiNewJobTab", "Determine the tab name of a job (HopGuiJobGraph)" ),
  ;

  public String id;

  public String description;

  public Class<?> providedClass;

  private HopExtensionPoint( String id, String description ) {
    this.id = id;
    this.description = description;
    this.providedClass = Object.class;
  }

  private HopExtensionPoint( String id, String description, Class<?> providedClass ) {
    this.id = id;
    this.description = description;
    this.providedClass = providedClass;
  }
}
