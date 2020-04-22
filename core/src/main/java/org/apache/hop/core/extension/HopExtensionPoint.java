/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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

package org.apache.hop.core.extension;

public enum HopExtensionPoint {

  HopGuiStart( "HopGuiStart", "HopGui has started" ),
  OpenRecent( "OpenRecent", "A recent file is opened" ),

  PipelinePrepareExecution( "PipelinePrepareExecution", "A Pipeline begins to prepare execution" ),
  PipelineStartThreads( "PipelineStartThreads", "A Pipeline begins to start" ),
  PipelineStart( "PipelineStart", "A Pipeline has started" ),
  PipelineHeartbeat( "PipelineHeartbeat", "A signal sent at regular intervals to indicate that the Pipeline is still active" ),
  PipelineFinish( "PipelineFinish", "A Pipeline finishes" ),
  PipelineMetaLoaded( "PipelineMetaLoaded", "Pipeline metadata was loaded" ),
  PipelinePainterArrow( "PipelinePainterArrow", "Draw additional information on top of a Pipeline hop (arrow)" ),
  PipelinePainterTransform( "PipelinePainterTransform", "Draw additional information on top of a Pipeline transform icon" ),
  PipelinePainterFlyout( "PipelinePainterFlyout", "Draw transform flyout when transform is clicked" ),
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
  PipelineTransformRightClick( "PipelineTransformRightClick", "A right button was clicked on a transform" ),
  PipelineGraphMouseMoved( "PipelineGraphMouseMoved", "The mouse was moved on the canvas" ),
  PipelineGraphMouseDoubleClick( "PipelineGraphMouseDoubleClick", "A left or right button was double-clicked in a Pipeline" ),
  PipelineBeforeDeleteTransforms( "PipelineBeforeDeleteTransforms", "Pipeline transforms about to be deleted" ),
  HopGuiPipelineMetaExecutionStart( "HopGuiPipelineMetaExecutionStart", "Hop GUI initiates the execution of a pipeline (PipelineMeta)" ),
  HopUiPipelineExecutionConfiguration( "HopUiPipelineExecutionConfiguration", "Right before Hop UI configuration of Pipeline to be executed takes place" ),
  HopUiPipelineBeforeStart( "HopUiPipelineBeforeStart", "Right before the Pipeline is started" ),
  HopUiJobBeforeStart( "HopUiJobBeforeStart", "Right before the workflow is started" ),
  RunConfigurationSelection( "RunConfigurationSelection", "Check when run configuration is selected" ),
  RunConfigurationIsRemote( "RunConfigurationIsRemote", "Check when run configuration is pointing to a remote server" ),
  HopUiRunConfiguration( "HopUiRunConfiguration", "Send the run configuration" ),
  JobStart( "JobStart", "A workflow starts" ),
  JobHeartbeat( "JobHeartbeat", "A signal sent at regular intervals to indicate that the workflow is still active" ),
  JobFinish( "JobFinish", "A workflow finishes" ),
  JobBeforeJobEntryExecution( "JobBeforeActionExecution", "Before a action executes" ),
  JobAfterJobEntryExecution( "JobAfterActionExecution", "After a action executes" ),
  JobBeginProcessing( "JobBeginProcessing", "Start of a workflow at the end of the log table handling" ),
  JobPainterArrow( "JobPainterArrow", "Draw additional information on top of a workflow hop (arrow)" ),
  JobPainterJobEntry( "PipelinePainterAction", "Draw additional information on top of a action copy icon" ),
  JobPainterStart( "JobPainterStart", "Draw workflow or plugin metadata at the start (below the rest)" ),
  JobPainterEnd( "JobPainterEnd", "Draw workflow or plugin metadata at the end (on top of all the rest)" ),
  JobGraphMouseDown( "JobGraphMouseDown", "A left or right button was clicked in a workflow" ),
  JobBeforeOpen( "JobBeforeOpen", "A workflow file is about to be opened" ),
  JobAfterOpen( "JobAfterOpen", "A workflow file was opened" ),
  JobBeforeSave( "JobBeforeSave", "A workflow file is about to be saved" ),
  JobAfterSave( "JobAfterSave", "A workflow file was saved" ),
  JobBeforeClose( "JobBeforeClose", "A workflow file is about to be closed" ),
  JobAfterClose( "JobAfterClose", "A workflow file was closed" ),
  JobChanged( "JobChanged", "A workflow has been changed" ),
  JobGraphMouseDoubleClick( "JobGraphMouseDoubleClick", "A left or right button was double-clicked in a workflow" ),
  JobGraphJobEntrySetMenu( "JobGraphActionSetMenu", "Manipulate the menu on right click on a action" ),
  WorkflowDialogShowRetrieveLogTableFields( "WorkflowDialogShowRetrieveLogTableFields", "Show or retrieve the contents of the fields of a log channel on the log channel composite" ),
  JobEntryPipelineSave( "ActionPipelineSave", "Job entry Pipeline is saved" ),

  WorkflowMetaLoaded( "WorkflowMetaLoaded", "Job metadata was loaded" ),
  HopUiWorkflowMetaExecutionStart( "HopUiWorkflowMetaExecutionStart", "Hop UI initiates the execution of a workflow (WorkflowMeta)" ),
  HopUiJobExecutionConfiguration( "HopUiJobExecutionConfiguration", "Right before Hop UI configuration of workflow to be executed takes place" ),

  DatabaseConnected( "DatabaseConnected", "A connection to a database was made" ),
  DatabaseDisconnected( "DatabaseDisconnected", "A connection to a database was terminated" ),

  TransformBeforeInitialize( "TransformBeforeInitialize", "Right before a transform is about to be initialized" ),
  TransformAfterInitialize( "TransformAfterInitialize", "After a transform is initialized" ),

  TransformBeforeStart( "TransformBeforeStart", "Right before a transform is about to be started" ),
  TransformFinished( "TransformFinished", "After a transform has finished" ),

  BeforeCheckTransforms( "BeforeCheckTransforms", "Right before a set of transforms is about to be verified." ),
  AfterCheckTransforms( "AfterCheckTransforms", "After a set of transforms has been checked for warnings/errors." ),
  BeforeCheckTransform( "BeforeCheckTransform", "Right before a transform is about to be verified." ),
  AfterCheckTransform( "AfterCheckTransform", "After a transform has been checked for warnings/errors." ),

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
  HopGuiNewJobTab( "HopGuiNewJobTab", "Determine the tab name of a workflow (HopGuiJobGraph)" ),
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
