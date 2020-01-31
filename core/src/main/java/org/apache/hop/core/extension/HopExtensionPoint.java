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
  // Some transformation points
  //
  TransformationPrepareExecution( "TransformationPrepareExecution", "A transformation begins to prepare execution" ),
  TransformationStartThreads( "TransformationStartThreads", "A transformation begins to start" ),
  TransformationStart( "TransformationStart", "A transformation has started" ),
  TransformationHeartbeat( "TransformationHeartbeat",
    "A signal sent at regular intervals to indicate that the transformation is still active" ),
  TransformationFinish( "TransformationFinish", "A transformation finishes" ),
  TransformationMetaLoaded( "TransformationMetaLoaded", "Transformation metadata was loaded" ),
  TransPainterArrow( "TransPainterArrow", "Draw additional information on top of a transformation hop (arrow)" ),
  TransPainterStep( "TransPainterStep", "Draw additional information on top of a transformation step icon" ),
  TransPainterFlyout( "TransPainterFlyout", "Draw step flyout when step is clicked" ),
  TransPainterFlyoutTooltip( "TransPainterFlyoutTooltip", "Draw the flyout tooltips" ),
  TransPainterStart( "TransPainterStart", "Draw transformation or plugin metadata at the start (below the rest)" ),
  TransPainterEnd( "TransPainterEnd", "Draw transformation or plugin metadata at the end (on top of all the rest)" ),
  TransGraphMouseDown( "TransGraphMouseDown", "A mouse down event occurred on the canvas" ),
  TransGraphMouseUp( "TransGraphMouseUp", "A mouse up event occurred on the canvas" ),
  TransBeforeOpen( "TransBeforeOpen", "A transformation file is about to be opened" ),
  TransAfterOpen( "TransAfterOpen", "A transformation file was opened" ),
  TransBeforeSave( "TransBeforeSave", "A transformation file is about to be saved" ),
  TransAfterSave( "TransAfterSave", "A transformation file was saved" ),
  TransBeforeClose( "TransBeforeClose", "A transformation file is about to be closed" ),
  TransAfterClose( "TransAfterClose", "A transformation file was closed" ),
  TransChanged( "TransChanged", "A transformation has been changed" ),
  TransStepRightClick( "TransStepRightClick", "A right button was clicked on a step" ),
  TransGraphMouseMoved( "TransGraphMouseMoved", "The mouse was moved on the canvas" ),
  TransGraphMouseDoubleClick( "TransGraphMouseDoubleClick",
    "A left or right button was double-clicked in a transformation" ),
  TransBeforeDeleteSteps( "TransBeforeDeleteSteps", "Transformation steps about to be deleted" ),
  HopUiTransMetaExecutionStart( "HopUiTransMetaExecutionStart",
    "Hop UI initiates the execution of a trans (TransMeta)" ),
  HopUiTransExecutionConfiguration( "HopUiTransExecutionConfiguration",
    "Right before Hop UI configuration of transformation to be executed takes place" ),
  HopUiTransBeforeStart( "HopUiTransBeforeStart", "Right before the transformation is started" ),
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
  JobPainterJobEntry( "TransPainterJobEntry", "Draw additional information on top of a job entry copy icon" ),
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
  JobGraphMouseDoubleClick( "JobGraphMouseDoubleClick",
    "A left or right button was double-clicked in a job" ),
  JobGraphJobEntrySetMenu( "JobGraphJobEntrySetMenu", "Manipulate the menu on right click on a job entry" ),
  JobDialogShowRetrieveLogTableFields( "JobDialogShowRetrieveLogTableFields",
    "Show or retrieve the contents of the fields of a log channel on the log channel composite" ),
  JobEntryTransSave( "JobEntryTransSave", "Job entry trans is saved" ),

  JobMetaLoaded( "JobMetaLoaded", "Job metadata was loaded" ),
  HopUiJobMetaExecutionStart( "HopUiJobMetaExecutionStart", "Hop UI initiates the execution of a job (JobMeta)" ),
  HopUiJobExecutionConfiguration( "HopUiJobExecutionConfiguration",
    "Right before Hop UI configuration of job to be executed takes place" ),

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

  CarteStartup( "CarteStartup", "Right after the Carte webserver has started and is fully functional" ),
  CarteShutdown( "CarteShutdown", "Right before the Carte webserver will shut down" ),

  HopUiViewTreeExtension( "HopUiViewTreeExtension", "View tree Hop UI extension" ),
  HopUiPopupMenuExtension( "HopUiPopupMenuExtension", "Pop up menu extension for the view tree" ),
  HopUiTreeDelegateExtension( "HopUiTreeDelegateExtension", "During the HopUiTreeDelegate execution" ),
  HopUiBrowserFunction( "HopUiBrowserFunction", "Generic browser function handler" ),
  GetFieldsExtension( "GetFieldsExtension", "Get Fields dialog" ),

  OpenMapping( "OpenMapping", "Trigger when opening a mapping from TransGraph" ),

  TransformationCreateNew( "TransformationCreateNew", "Create a New Empty Transformation in the Hop UI" ),

  HopGuiFileOpenDialog( "HopGuiFileOpenDialog", "Allows you to modify the file dialog before it's shown. If you want to show your own, set doIt to false ("),
  HopGuiNewTransformationTab( "HopGuiTransformationTab", "Determine the tab name of a transformation (HopGuiTransGraph)" ),
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
