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
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
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

  protected ActionPipeline jobEntry;

  private static final String[] FILE_FILTERLOGNAMES = new String[] {
    BaseMessages.getString( PKG, "JobPipeline.Fileformat.TXT" ),
    BaseMessages.getString( PKG, "JobPipeline.Fileformat.LOG" ),
    BaseMessages.getString( PKG, "JobPipeline.Fileformat.All" ) };

  public ActionPipelineDialog( Shell parent, IAction jobEntryInt, WorkflowMeta workflowMeta ) {
    super( parent, jobEntryInt, workflowMeta );
    jobEntry = (ActionPipeline) jobEntryInt;
  }

  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell( parent, props.getWorkflowsDialogStyle() );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, jobEntry );

    backupChanged = jobEntry.hasChanged();

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
    return jobEntry;
  }

  protected void createElements() {
    super.createElements();
    shell.setText( BaseMessages.getString( PKG, "JobPipeline.Header" ) );

    wlPath.setText( BaseMessages.getString( PKG, "JobPipeline.JobTransform.Pipeline.Label" ) );
    wPassParams.setText( BaseMessages.getString( PKG, "JobPipeline.PassAllParameters.Label" ) );

    wClearRows = new Button( gExecution, SWT.CHECK );
    props.setLook( wClearRows );
    wClearRows.setText( BaseMessages.getString( PKG, "JobPipeline.ClearResultList.Label" ) );
    FormData fdbClearRows = new FormData();
    fdbClearRows.left = new FormAttachment( 0, 0 );
    fdbClearRows.top = new FormAttachment( wEveryRow, 10 );
    wClearRows.setLayoutData( fdbClearRows );

    wClearFiles = new Button( gExecution, SWT.CHECK );
    props.setLook( wClearFiles );
    wClearFiles.setText( BaseMessages.getString( PKG, "JobPipeline.ClearResultFiles.Label" ) );
    FormData fdbClearFiles = new FormData();
    fdbClearFiles.left = new FormAttachment( 0, 0 );
    fdbClearFiles.top = new FormAttachment( wClearRows, 10 );
    wClearFiles.setLayoutData( fdbClearFiles );

    wWaitingToFinish = new Button( gExecution, SWT.CHECK );
    props.setLook( wWaitingToFinish );
    wWaitingToFinish.setText( BaseMessages.getString( PKG, "JobPipeline.WaitToFinish.Label" ) );
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment( wClearFiles, 10 );
    fdWait.left = new FormAttachment( 0, 0 );
    wWaitingToFinish.setLayoutData( fdWait );

    wFollowingAbortRemotely = new Button( gExecution, SWT.CHECK );
    props.setLook( wFollowingAbortRemotely );
    wFollowingAbortRemotely.setText( BaseMessages.getString( PKG, "JobPipeline.AbortRemote.Label" ) );
    FormData fdFollow = new FormData();
    fdFollow.top = new FormAttachment( wWaitingToFinish, 10 );
    fdFollow.left = new FormAttachment( 0, 0 );
    wFollowingAbortRemotely.setLayoutData( fdFollow );

    Composite cRunConfiguration = new Composite( wOptions, SWT.NONE );
    cRunConfiguration.setLayout( new FormLayout() );
    props.setLook( cRunConfiguration );
    FormData fdLocal = new FormData();
    fdLocal.top = new FormAttachment( 0 );
    fdLocal.right = new FormAttachment( 100 );
    fdLocal.left = new FormAttachment( 0 );

    cRunConfiguration.setBackground( shell.getBackground() ); // the default looks ugly
    cRunConfiguration.setLayoutData( fdLocal );

    Label wlRunConfiguration = new Label( cRunConfiguration, SWT.LEFT );
    props.setLook( wlRunConfiguration );
    wlRunConfiguration.setText( "Run configuration:" );
    FormData fdlRunConfiguration = new FormData();
    fdlRunConfiguration.top = new FormAttachment( 0 );
    fdlRunConfiguration.left = new FormAttachment( 0 );
    wlRunConfiguration.setLayoutData( fdlRunConfiguration );

    wRunConfiguration = new ComboVar( workflowMeta, cRunConfiguration, SWT.BORDER );
    props.setLook( wRunConfiguration );
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.width = 200;
    fdRunConfiguration.top = new FormAttachment( wlRunConfiguration, 5 );
    fdRunConfiguration.left = new FormAttachment( 0 );
    wRunConfiguration.setLayoutData( fdRunConfiguration );
    wRunConfiguration.addModifyListener( new RunConfigurationModifyListener() );

    fdgExecution.top = new FormAttachment( cRunConfiguration, 10 );

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

  protected ActionBase getJobEntry() {
    return jobEntry;
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage( shell.getDisplay(), getClass().getClassLoader(), "PPL.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE );
  }

  protected String[] getParameters() {
    return jobEntry.parameters;
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
            mb.setMessage( BaseMessages.getString( PKG, "JobPipeline.Dialog.CreatePipelineQuestion.Message" ) );
            mb.setText( BaseMessages.getString( PKG, "JobPipeline.Dialog.CreatePipelineQuestion.Title" ) ); // Sorry!
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
    wName.setText( Const.NVL( jobEntry.getName(), "" ) );

    wPath.setText( Const.NVL( jobEntry.getFilename(), "" ) );

    // Parameters
    if ( jobEntry.parameters != null ) {
      for ( int i = 0; i < jobEntry.parameters.length; i++ ) {
        TableItem ti = wParameters.table.getItem( i );
        if ( !Utils.isEmpty( jobEntry.parameters[ i ] ) ) {
          ti.setText( 1, Const.NVL( jobEntry.parameters[ i ], "" ) );
          ti.setText( 2, Const.NVL( jobEntry.parameterFieldNames[ i ], "" ) );
          ti.setText( 3, Const.NVL( jobEntry.parameterValues[ i ], "" ) );
        }
      }
      wParameters.setRowNums();
      wParameters.optWidth( true );
    }

    wPassParams.setSelection( jobEntry.isPassingAllParameters() );

    if ( jobEntry.logfile != null ) {
      wLogfile.setText( jobEntry.logfile );
    }
    if ( jobEntry.logext != null ) {
      wLogext.setText( jobEntry.logext );
    }

    wPrevToParams.setSelection( jobEntry.paramsFromPrevious );
    wEveryRow.setSelection( jobEntry.execPerRow );
    wSetLogfile.setSelection( jobEntry.setLogfile );
    wAddDate.setSelection( jobEntry.addDate );
    wAddTime.setSelection( jobEntry.addTime );
    wClearRows.setSelection( jobEntry.clearResultRows );
    wClearFiles.setSelection( jobEntry.clearResultFiles );
    wWaitingToFinish.setSelection( jobEntry.isWaitingToFinish() );
    wFollowingAbortRemotely.setSelection( jobEntry.isFollowingAbortRemotely() );
    wAppendLogfile.setSelection( jobEntry.setAppendLogfile );

    wbLogFilename.setSelection( jobEntry.setAppendLogfile );

    wCreateParentFolder.setSelection( jobEntry.createParentFolder );
    if ( jobEntry.logFileLevel != null ) {
      wLoglevel.select( jobEntry.logFileLevel.getLevel() );
    }

    List<String> runConfigurations = new ArrayList<>();
    try {
      ExtensionPointHandler
        .callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopUiRunConfiguration.id,
          new Object[] { runConfigurations, PipelineMeta.XML_TAG } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    wRunConfiguration.setItems( runConfigurations.toArray( new String[ 0 ] ) );
    if ( Utils.isEmpty( jobEntry.getRunConfiguration() ) ) {
      wRunConfiguration.select( 0 );
    } else {
      wRunConfiguration.setText( jobEntry.getRunConfiguration() );
    }

    wName.selectAll();
    wName.setFocus();
  }

  protected void cancel() {
    jobEntry.setChanged( backupChanged );

    jobEntry = null;
    dispose();
  }

  private void getInfo( ActionPipeline jet ) throws HopException {
    jet.setName( wName.getText() );
    jet.setFileName( wPath.getText() );
    if ( jet.getFilename().isEmpty() ) {
      throw new HopException( BaseMessages.getString( PKG,
        "JobPipeline.Dialog.Exception.NoValidMappingDetailsFound" ) );
    }

    // Do the parameters
    int nritems = wParameters.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      if ( param != null && param.length() != 0 ) {
        nr++;
      }
    }
    jet.parameters = new String[ nr ];
    jet.parameterFieldNames = new String[ nr ];
    jet.parameterValues = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      String fieldName = wParameters.getNonEmpty( i ).getText( 2 );
      String value = wParameters.getNonEmpty( i ).getText( 3 );

      jet.parameters[ nr ] = param;

      if ( !Utils.isEmpty( Const.trim( fieldName ) ) ) {
        jet.parameterFieldNames[ nr ] = fieldName;
      } else {
        jet.parameterFieldNames[ nr ] = "";
      }

      if ( !Utils.isEmpty( Const.trim( value ) ) ) {
        jet.parameterValues[ nr ] = value;
      } else {
        jet.parameterValues[ nr ] = "";
      }

      nr++;
    }

    jet.setPassingAllParameters( wPassParams.getSelection() );

    jet.logfile = wLogfile.getText();
    jet.logext = wLogext.getText();

    if ( wLoglevel.getSelectionIndex() >= 0 ) {
      jet.logFileLevel = LogLevel.values()[ wLoglevel.getSelectionIndex() ];
    } else {
      jet.logFileLevel = LogLevel.BASIC;
    }

    jet.paramsFromPrevious = wPrevToParams.getSelection();
    jet.execPerRow = wEveryRow.getSelection();
    jet.setLogfile = wSetLogfile.getSelection();
    jet.addDate = wAddDate.getSelection();
    jet.addTime = wAddTime.getSelection();
    jet.clearResultRows = wClearRows.getSelection();
    jet.clearResultFiles = wClearFiles.getSelection();
    jet.createParentFolder = wCreateParentFolder.getSelection();
    jet.setRunConfiguration( wRunConfiguration.getText() );
    jet.setAppendLogfile = wAppendLogfile.getSelection();
    jet.setWaitingToFinish( wWaitingToFinish.getSelection() );
    jet.setFollowingAbortRemotely( wFollowingAbortRemotely.getSelection() );

    PipelineExecutionConfiguration executionConfiguration = new PipelineExecutionConfiguration();
    executionConfiguration.setRunConfiguration( jet.getRunConfiguration() );
    try {
      ExtensionPointHandler.callExtensionPoint( jobEntry.getLogChannel(), HopExtensionPoint.HopUiPipelineBeforeStart.id,
        new Object[] { executionConfiguration, workflowMeta, workflowMeta, null } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    try {
      ExtensionPointHandler.callExtensionPoint( jobEntry.getLogChannel(), HopExtensionPoint.JobEntryPipelineSave.id,
        new Object[] { workflowMeta, jet.getRunConfiguration() } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    jet.setLoggingRemoteWork( executionConfiguration.isLogRemoteExecutionLocally() );
  }

  protected void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      final Dialog dialog = new SimpleMessageDialog( shell,
        BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ),
        BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ), MessageDialog.ERROR );
      dialog.open();
      return;
    }
    jobEntry.setName( wName.getText() );

    try {
      getInfo( jobEntry );
    } catch ( HopException e ) {
      // suppress exceptions at this time - we will let the runtime report on any errors
    }
    jobEntry.setChanged();
    dispose();
  }
}
