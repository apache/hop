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
  PipelinePainterArrow( "Draw additional information on top of a Pipeline hop (arrow)" ),
  PipelinePainterTransform( "Draw additional information on top of a Pipeline transform icon" ),
  PipelinePainterFlyout( "Draw transform flyout when transform is clicked" ),
  PipelinePainterFlyoutTooltip( "Draw the flyout tooltips" ),
  PipelinePainterStart( "Draw Pipeline or plugin metadata at the start (below the rest)" ),
  PipelinePainterEnd( "Draw Pipeline or plugin metadata at the end (on top of all the rest)" ),
  PipelineGraphMouseDown( "A mouse down event occurred on the canvas" ),
  PipelineGraphMouseUp( "A mouse up event occurred on the canvas" ),
  PipelineBeforeOpen( "A Pipeline file is about to be opened" ),
  PipelineAfterOpen( "A Pipeline file was opened" ),
  PipelineBeforeSave( "A Pipeline file is about to be saved" ),
  PipelineAfterSave( "A Pipeline file was saved" ),
  PipelineBeforeClose( "A Pipeline file is about to be closed" ),
  PipelineAfterClose( "A Pipeline file was closed" ),
  PipelineChanged( "A Pipeline has been changed" ),
  PipelineTransformRightClick( "A right button was clicked on a transform" ),
  PipelineGraphMouseMoved( "The mouse was moved on the canvas" ),
  PipelineGraphMouseDoubleClick( "A left or right button was double-clicked in a Pipeline" ),
  PipelineBeforeDeleteTransforms( "Pipeline transforms about to be deleted" ),
  HopGuiPipelineMetaExecutionStart( "Hop GUI initiates the execution of a pipeline (PipelineMeta)" ),
  HopUiPipelineExecutionConfiguration( "Right before Hop UI configuration of Pipeline to be executed takes place" ),
  HopUiPipelineBeforeStart( "Right before the Pipeline is started" ),
  HopUiJobBeforeStart( "Right before the workflow is started" ),
  RunConfigurationSelection( "Check when run configuration is selected" ),
  RunConfigurationIsRemote( "Check when run configuration is pointing to a remote server" ),
  HopUiRunConfiguration( "Send the run configuration" ),
  JobStart( "A workflow starts" ),
  JobHeartbeat( "A signal sent at regular intervals to indicate that the workflow is still active" ),
  JobFinish( "A workflow finishes" ),
  JobBeforeJobEntryExecution( "Before a action executes" ),
  JobAfterJobEntryExecution( "After a action executes" ),
  JobBeginProcessing( "Start of a workflow at the end of the log table handling" ),
  JobPainterArrow( "Draw additional information on top of a workflow hop (arrow)" ),
  JobPainterJobEntry( "Draw additional information on top of a action copy icon" ),
  JobPainterStart( "Draw workflow or plugin metadata at the start (below the rest)" ),
  JobPainterEnd( "Draw workflow or plugin metadata at the end (on top of all the rest)" ),
  JobGraphMouseDown( "A left or right button was clicked in a workflow" ),
  JobBeforeOpen( "A workflow file is about to be opened" ),
  JobAfterOpen( "A workflow file was opened" ),
  JobBeforeSave( "A workflow file is about to be saved" ),
  JobAfterSave( "A workflow file was saved" ),
  JobBeforeClose( "A workflow file is about to be closed" ),
  JobAfterClose( "A workflow file was closed" ),
  JobChanged( "A workflow has been changed" ),
  JobGraphMouseDoubleClick( "A left or right button was double-clicked in a workflow" ),
  JobGraphJobEntrySetMenu( "Manipulate the menu on right click on a action" ),
  WorkflowDialogShowRetrieveLogTableFields( "Show or retrieve the contents of the fields of a log channel on the log channel composite" ),
  JobEntryPipelineSave( "Job entry Pipeline is saved" ),

  WorkflowMetaLoaded( "Job metadata was loaded" ),
  HopUiWorkflowMetaExecutionStart( "Hop UI initiates the execution of a workflow (WorkflowMeta)" ),
  HopUiJobExecutionConfiguration( "Right before Hop UI configuration of workflow to be executed takes place" ),

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
  HopGuiNewPipelineTab( "Determine the tab name of a Pipeline (HopGuiPipelineGraph)" ),
  HopGuiNewJobTab( "Determine the tab name of a workflow (HopGuiJobGraph)" ),

  HopGuiMetaStoreElementCreated("A new metastore element is created"),
  HopGuiMetaStoreElementUpdated("A metastore element is updated"),
  HopGuiMetaStoreElementDeleted("A metastore element is deleted"),
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
