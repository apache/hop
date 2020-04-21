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

package org.apache.hop.ui.workflow.actions.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SimpleMessageDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * This dialog allows you to edit the pipeline action (ActionPipeline)
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionPipelineDialog extends ActionBaseDialog implements IActionDialog {
  private static Class<?> PKG = ActionPipeline.class; // for i18n purposes, needed by Translator!!

  protected ActionPipeline action;

  private static final String[] FILE_FILTERLOGNAMES = new String[] {
    BaseMessages.getString( PKG, "ActionPipeline.Fileformat.TXT" ),
    BaseMessages.getString( PKG, "ActionPipeline.Fileformat.LOG" ),
    BaseMessages.getString( PKG, "ActionPipeline.Fileformat.All" ) };

  public ActionPipelineDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionPipeline) action;
  }

  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell( parent, props.getWorkflowsDialogStyle() );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    backupChanged = action.hasChanged();

    createElements();

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setActive();

    BaseTransformDialog.setSize( shell );

    int width = 750;
    int height = Const.isWindows() ? 730 : 720;

    shell.setSize( width, height );
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  protected void createElements() {
    super.createElements();
    shell.setText( BaseMessages.getString( PKG, "ActionPipeline.Header" ) );

    wlPath.setText( BaseMessages.getString( PKG, "ActionPipeline.Filename.Label" ) );
    wPassParams.setText( BaseMessages.getString( PKG, "ActionPipeline.PassAllParameters.Label" ) );

    wClearRows = new Button( gExecution, SWT.CHECK );
    props.setLook( wClearRows );
    wClearRows.setText( BaseMessages.getString( PKG, "ActionPipeline.ClearResultList.Label" ) );
    FormData fdbClearRows = new FormData();
    fdbClearRows.left = new FormAttachment( 0, 0 );
    fdbClearRows.top = new FormAttachment( wEveryRow, 10 );
    wClearRows.setLayoutData( fdbClearRows );

    wClearFiles = new Button( gExecution, SWT.CHECK );
    props.setLook( wClearFiles );
    wClearFiles.setText( BaseMessages.getString( PKG, "ActionPipeline.ClearResultFiles.Label" ) );
    FormData fdbClearFiles = new FormData();
    fdbClearFiles.left = new FormAttachment( 0, 0 );
    fdbClearFiles.top = new FormAttachment( wClearRows, 10 );
    wClearFiles.setLayoutData( fdbClearFiles );

    wWaitingToFinish = new Button( gExecution, SWT.CHECK );
    props.setLook( wWaitingToFinish );
    wWaitingToFinish.setText( BaseMessages.getString( PKG, "ActionPipeline.WaitToFinish.Label" ) );
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment( wClearFiles, 10 );
    fdWait.left = new FormAttachment( 0, 0 );
    wWaitingToFinish.setLayoutData( fdWait );

    wFollowingAbortRemotely = new Button( gExecution, SWT.CHECK );
    props.setLook( wFollowingAbortRemotely );
    wFollowingAbortRemotely.setText( BaseMessages.getString( PKG, "ActionPipeline.AbortRemote.Label" ) );
    FormData fdFollow = new FormData();
    fdFollow.top = new FormAttachment( wWaitingToFinish, 10 );
    fdFollow.left = new FormAttachment( 0, 0 );
    wFollowingAbortRemotely.setLayoutData( fdFollow );

    wbGetParams.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        getParameters( null ); // force reload from file specification
      }
    } );

    wbBrowse.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        pickFileVFS();
      }
    } );

    wbLogFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        selectLogFile( FILE_FILTERLOGNAMES );
      }
    } );
  }

  protected ActionBase getAction() {
    return action;
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage( shell.getDisplay(), getClass().getClassLoader(), "PPL.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE );
  }

  protected String[] getParameters() {
    return action.parameters;
  }

  private void getParameters( PipelineMeta inputPipelineMeta ) {
    try {
      if ( inputPipelineMeta == null ) {
        ActionPipeline jet = new ActionPipeline();
        getInfo( jet );
        inputPipelineMeta = jet.getPipelineMeta( metaStore, workflowMeta );
      }
      String[] parameters = inputPipelineMeta.listParameters();

      String[] existing = wParameters.getItems( 1 );

      for ( int i = 0; i < parameters.length; i++ ) {
        if ( Const.indexOfString( parameters[ i ], existing ) < 0 ) {
          TableItem item = new TableItem( wParameters.table, SWT.NONE );
          item.setText( 1, parameters[ i ] );
        }
      }
      wParameters.removeEmptyRows();
      wParameters.setRowNums();
      wParameters.optWidth( true );
    } catch ( Exception e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "ActionPipelineDialog.Exception.UnableToLoadPipeline.Title" ),
        BaseMessages.getString( PKG, "ActionPipelineDialog.Exception.UnableToLoadPipeline.Message" ), e );
    }

  }

  protected void pickFileVFS() {

    HopPipelineFileType<PipelineMeta> pipelineFileType = HopDataOrchestrationPerspective.getInstance().getPipelineFileType();

    FileDialog dialog = new FileDialog( shell, SWT.OPEN );
    dialog.setFilterExtensions( pipelineFileType.getFilterExtensions() );
    dialog.setFilterNames( pipelineFileType.getFilterNames() );
    String prevName = workflowMeta.environmentSubstitute( wPath.getText() );
    String parentFolder = null;
    try {
      parentFolder = HopVfs.getFilename( HopVfs.getFileObject( workflowMeta.environmentSubstitute( workflowMeta.getFilename() ) ).getParent() );
    } catch ( Exception e ) {
      // not that important
    }
    if ( !Utils.isEmpty( prevName ) ) {
      try {
        if ( HopVfs.fileExists( prevName ) ) {
          dialog.setFilterPath( HopVfs.getFilename( HopVfs.getFileObject( prevName ).getParent() ) );
        } else {

          if ( !prevName.endsWith( ".hpl" ) ) {
            prevName = getEntryName( Const.trim( wPath.getText() ) + ".hpl" );
          }
          if ( HopVfs.fileExists( prevName ) ) {
            wPath.setText( prevName );
            return;
          } else {
            // File specified doesn't exist. Ask if we should create the file...
            //
            MessageBox mb = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION );
            mb.setMessage( BaseMessages.getString( PKG, "ActionPipeline.Dialog.CreatePipelineQuestion.Message" ) );
            mb.setText( BaseMessages.getString( PKG, "ActionPipeline.Dialog.CreatePipelineQuestion.Title" ) ); // Sorry!
            int answer = mb.open();
            if ( answer == SWT.YES ) {

              HopGui hopGui = HopGui.getInstance();
              IHopFileTypeHandler fileTypeHandler = HopDataOrchestrationPerspective.getInstance().getPipelineFileType().newFile( hopGui, hopGui.getVariables() );
              fileTypeHandler.setFilename( workflowMeta.environmentSubstitute( prevName ) );
              wPath.setText( prevName );
              hopGui.fileDelegate.fileSave();
              return;
            }
          }
        }
      } catch ( Exception e ) {
        dialog.setFilterPath( parentFolder );
      }
    } else if ( !Utils.isEmpty( parentFolder ) ) {
      dialog.setFilterPath( parentFolder );
    }

    String fname = dialog.open();
    if ( fname != null ) {
      File file = new File( fname );
      String name = file.getName();
      String parentFolderSelection = file.getParentFile().toString();

      if ( !Utils.isEmpty( parentFolder ) && parentFolder.equals( parentFolderSelection ) ) {
        wPath.setText( getEntryName( name ) );
      } else {
        wPath.setText( fname );
      }

    }
  }

  String getEntryName( String name ) {
    return "${"
      + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + name;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( action.getName(), "" ) );
    wPath.setText( Const.NVL( action.getFilename(), "" ) );

    // Parameters
    if ( action.parameters != null ) {
      for ( int i = 0; i < action.parameters.length; i++ ) {
        TableItem ti = wParameters.table.getItem( i );
        if ( !Utils.isEmpty( action.parameters[ i ] ) ) {
          ti.setText( 1, Const.NVL( action.parameters[ i ], "" ) );
          ti.setText( 2, Const.NVL( action.parameterFieldNames[ i ], "" ) );
          ti.setText( 3, Const.NVL( action.parameterValues[ i ], "" ) );
        }
      }
      wParameters.setRowNums();
      wParameters.optWidth( true );
    }

    wPassParams.setSelection( action.isPassingAllParameters() );

    if ( action.logfile != null ) {
      wLogfile.setText( action.logfile );
    }
    if ( action.logext != null ) {
      wLogext.setText( action.logext );
    }

    wPrevToParams.setSelection( action.paramsFromPrevious );
    wEveryRow.setSelection( action.execPerRow );
    wSetLogfile.setSelection( action.setLogfile );
    wAddDate.setSelection( action.addDate );
    wAddTime.setSelection( action.addTime );
    wClearRows.setSelection( action.clearResultRows );
    wClearFiles.setSelection( action.clearResultFiles );
    wWaitingToFinish.setSelection( action.isWaitingToFinish() );
    wFollowingAbortRemotely.setSelection( action.isFollowingAbortRemotely() );
    wAppendLogfile.setSelection( action.setAppendLogfile );

    wbLogFilename.setSelection( action.setAppendLogfile );

    wCreateParentFolder.setSelection( action.createParentFolder );
    if ( action.logFileLevel != null ) {
      wLoglevel.select( action.logFileLevel.getLevel() );
    }

    try {
      List<String> runConfigurations = PipelineRunConfiguration.createFactory( metaStore).getElementNames();

      try {
        ExtensionPointHandler.callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopUiRunConfiguration.id, new Object[] { runConfigurations, PipelineMeta.XML_TAG } );
      } catch ( HopException e ) {
        // Ignore errors
      }

      wRunConfiguration.setItems(runConfigurations.toArray( new String[0] ));
      wRunConfiguration.setText( Const.NVL(action.getRunConfiguration(), "") );

      if ( Utils.isEmpty( action.getRunConfiguration() ) ) {
        wRunConfiguration.select( 0 );
      } else {
        wRunConfiguration.setText( action.getRunConfiguration() );
      }
    } catch(Exception e) {
      LogChannel.UI.logError( "Error getting pipeline run configurations", e );
    }

    wName.selectAll();
    wName.setFocus();
  }

  protected void cancel() {
    action.setChanged( backupChanged );

    action = null;
    dispose();
  }

  private void getInfo( ActionPipeline ap ) throws HopException {
    ap.setName( wName.getText() );
    ap.setRunConfiguration( wRunConfiguration.getText() );
    ap.setFileName( wPath.getText() );
    if ( ap.getFilename().isEmpty() ) {
      throw new HopException( BaseMessages.getString( PKG,
        "ActionPipeline.Dialog.Exception.NoValidMappingDetailsFound" ) );
    }

    // Do the parameters
    int nrItems = wParameters.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      if ( param != null && param.length() != 0 ) {
        nr++;
      }
    }
    ap.parameters = new String[ nr ];
    ap.parameterFieldNames = new String[ nr ];
    ap.parameterValues = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      String fieldName = wParameters.getNonEmpty( i ).getText( 2 );
      String value = wParameters.getNonEmpty( i ).getText( 3 );

      ap.parameters[ nr ] = param;

      if ( !Utils.isEmpty( Const.trim( fieldName ) ) ) {
        ap.parameterFieldNames[ nr ] = fieldName;
      } else {
        ap.parameterFieldNames[ nr ] = "";
      }

      if ( !Utils.isEmpty( Const.trim( value ) ) ) {
        ap.parameterValues[ nr ] = value;
      } else {
        ap.parameterValues[ nr ] = "";
      }

      nr++;
    }

    ap.setPassingAllParameters( wPassParams.getSelection() );

    ap.logfile = wLogfile.getText();
    ap.logext = wLogext.getText();

    if ( wLoglevel.getSelectionIndex() >= 0 ) {
      ap.logFileLevel = LogLevel.values()[ wLoglevel.getSelectionIndex() ];
    } else {
      ap.logFileLevel = LogLevel.BASIC;
    }

    ap.paramsFromPrevious = wPrevToParams.getSelection();
    ap.execPerRow = wEveryRow.getSelection();
    ap.setLogfile = wSetLogfile.getSelection();
    ap.addDate = wAddDate.getSelection();
    ap.addTime = wAddTime.getSelection();
    ap.clearResultRows = wClearRows.getSelection();
    ap.clearResultFiles = wClearFiles.getSelection();
    ap.createParentFolder = wCreateParentFolder.getSelection();
    ap.setRunConfiguration( wRunConfiguration.getText() );
    ap.setAppendLogfile = wAppendLogfile.getSelection();
    ap.setWaitingToFinish( wWaitingToFinish.getSelection() );
    ap.setFollowingAbortRemotely( wFollowingAbortRemotely.getSelection() );

    PipelineExecutionConfiguration executionConfiguration = new PipelineExecutionConfiguration();
    executionConfiguration.setRunConfiguration( ap.getRunConfiguration() );
    try {
      ExtensionPointHandler.callExtensionPoint( this.action.getLogChannel(), HopExtensionPoint.HopUiPipelineBeforeStart.id,
        new Object[] { executionConfiguration, workflowMeta, workflowMeta, null } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    try {
      ExtensionPointHandler.callExtensionPoint( this.action.getLogChannel(), HopExtensionPoint.JobEntryPipelineSave.id,
        new Object[] { workflowMeta, ap.getRunConfiguration() } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    ap.setLoggingRemoteWork( executionConfiguration.isLogRemoteExecutionLocally() );
  }

  protected void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      final Dialog dialog = new SimpleMessageDialog( shell,
        BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ),
        BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ), MessageDialog.ERROR );
      dialog.open();
      return;
    }
    action.setName( wName.getText() );

    try {
      getInfo( action );
    } catch ( HopException e ) {
      // suppress exceptions at this time - we will let the runtime report on any errors
    }
    action.setChanged();
    dispose();
  }
}
