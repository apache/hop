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

package org.apache.hop.ui.workflow.actions.workflow;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.ui.workflow.actions.pipeline.ActionBaseDialog;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
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
 * This dialog allows you to edit the workflow action (ActionWorkflow)
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionWorkflowDialog extends ActionBaseDialog implements IActionDialog {
  private static Class<?> PKG = ActionWorkflow.class; // for i18n purposes, needed by Translator!!

  protected ActionWorkflow jobEntry;

  protected Button wPassExport;

  protected Button wExpandRemote;

  private static final String[] FILE_FILTERLOGNAMES = new String[] {
    BaseMessages.getString( PKG, "JobJob.Fileformat.TXT" ),
    BaseMessages.getString( PKG, "JobJob.Fileformat.LOG" ),
    BaseMessages.getString( PKG, "JobJob.Fileformat.All" ) };

  public ActionWorkflowDialog( Shell parent, IAction jobEntryInt, WorkflowMeta workflowMeta ) {
    super( parent, jobEntryInt, workflowMeta );
    jobEntry = (ActionWorkflow) jobEntryInt;
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
    int height = Const.isWindows() ? 730 : 718;

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
    shell.setText( BaseMessages.getString( PKG, "JobJob.Header" ) );

    wlPath.setText( BaseMessages.getString( PKG, "JobJob.JobTransform.Job.Label" ) );
    //wlDescription.setText( BaseMessages.getString( PKG, "JobJob.Local.Label" ) );
    wPassParams.setText( BaseMessages.getString( PKG, "JobJob.PassAllParameters.Label" ) );

    // Start Server Section
    wPassExport = new Button( gExecution, SWT.CHECK );
    wPassExport.setText( BaseMessages.getString( PKG, "JobJob.PassExportToSlave.Label" ) );
    props.setLook( wPassExport );
    FormData fdPassExport = new FormData();
    fdPassExport.left = new FormAttachment( 0, 0 );
    fdPassExport.top = new FormAttachment( wEveryRow, 10 );
    fdPassExport.right = new FormAttachment( 100, 0 );
    wPassExport.setLayoutData( fdPassExport );

    wExpandRemote = new Button( gExecution, SWT.CHECK );
    wExpandRemote.setText( BaseMessages.getString( PKG, "ActionWorkflowDialog.ExpandRemoteOnSlave.Label" ) );
    props.setLook( wExpandRemote );
    FormData fdExpandRemote = new FormData();
    fdExpandRemote.top = new FormAttachment( wPassExport, 10 );
    fdExpandRemote.left = new FormAttachment( 0, 0 );
    wExpandRemote.setLayoutData( fdExpandRemote );

    wWaitingToFinish = new Button( gExecution, SWT.CHECK );
    props.setLook( wWaitingToFinish );
    wWaitingToFinish.setText( BaseMessages.getString( PKG, "JobJob.WaitToFinish.Label" ) );
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment( wExpandRemote, 10 );
    fdWait.left = new FormAttachment( 0, 0 );
    wWaitingToFinish.setLayoutData( fdWait );

    wFollowingAbortRemotely = new Button( gExecution, SWT.CHECK );
    props.setLook( wFollowingAbortRemotely );
    wFollowingAbortRemotely.setText( BaseMessages.getString( PKG, "JobJob.AbortRemote.Label" ) );
    FormData fdFollow = new FormData();
    fdFollow.top = new FormAttachment( wWaitingToFinish, 10 );
    fdFollow.left = new FormAttachment( 0, 0 );
    wFollowingAbortRemotely.setLayoutData( fdFollow );
    // End Server Section

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
    return SwtSvgImageUtil.getImage( shell.getDisplay(), getClass().getClassLoader(), BasePropertyHandler.getProperty( "Workflow_image" ), ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE );
  }

  protected String[] getParameters() {
    return jobEntry.parameters;
  }

  protected void getParameters( WorkflowMeta inputWorkflowMeta ) {
    try {
      if ( inputWorkflowMeta == null ) {
        ActionWorkflow jej = new ActionWorkflow();
        getInfo( jej );
        inputWorkflowMeta = jej.getWorkflowMeta( metaStore, workflowMeta );
      }
      String[] parameters = inputWorkflowMeta.listParameters();

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
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "ActionWorkflowDialog.Exception.UnableToLoadJob.Title" ), BaseMessages
        .getString( PKG, "ActionWorkflowDialog.Exception.UnableToLoadJob.Message" ), e );
    }
  }

  protected void pickFileVFS() {

    HopWorkflowFileType<WorkflowMeta> jobFileType = HopDataOrchestrationPerspective.getInstance().getWorkflowFileType();

    FileDialog dialog = new FileDialog( shell, SWT.OPEN );
    dialog.setFilterExtensions( jobFileType.getFilterExtensions() );
    dialog.setFilterNames( jobFileType.getFilterNames() );
    String prevName = workflowMeta.environmentSubstitute( getPath() );
    String parentFolder = null;
    try {
      parentFolder =
        HopVfs.getFilename( HopVfs
          .getFileObject( workflowMeta.environmentSubstitute( workflowMeta.getFilename() ) ).getParent() );
    } catch ( Exception e ) {
      // not that important
    }
    if ( !Utils.isEmpty( prevName ) ) {
      try {
        if ( HopVfs.fileExists( prevName ) ) {
          dialog.setFilterPath( HopVfs.getFilename( HopVfs.getFileObject( prevName ).getParent() ) );
        } else {

          if ( !prevName.endsWith( ".hwf" ) ) {
            prevName = getEntryName( Const.trim( getPath() ) + ".hwf" );
          }
          if ( HopVfs.fileExists( prevName ) ) {
            wPath.setText( prevName );
            return;
          } else {
            // File specified doesn't exist. Ask if we should create the file...
            //
            MessageBox mb = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION );
            mb.setMessage( BaseMessages.getString( PKG, "JobJob.Dialog.CreateJobQuestion.Message" ) );
            mb.setText( BaseMessages.getString( PKG, "JobJob.Dialog.CreateJobQuestion.Title" ) ); // Sorry!
            int answer = mb.open();
            if ( answer == SWT.YES ) {

              HopGui hopGui = HopGui.getInstance();
              IHopFileTypeHandler typeHandler = HopDataOrchestrationPerspective.getInstance().getWorkflowFileType().newFile( hopGui, hopGui.getVariables() );
              typeHandler.setFilename( workflowMeta.environmentSubstitute( prevName ) );
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

  public void setActive() {
    super.setActive();
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

    wPrevToParams.setSelection( jobEntry.paramsFromPrevious );
    wEveryRow.setSelection( jobEntry.execPerRow );
    wSetLogfile.setSelection( jobEntry.setLogfile );
    if ( jobEntry.logfile != null ) {
      wLogfile.setText( jobEntry.logfile );
    }
    if ( jobEntry.logext != null ) {
      wLogext.setText( jobEntry.logext );
    }
    wAddDate.setSelection( jobEntry.addDate );
    wAddTime.setSelection( jobEntry.addTime );
    wPassExport.setSelection( jobEntry.isPassingExport() );

    if ( jobEntry.logFileLevel != null ) {
      wLoglevel.select( jobEntry.logFileLevel.getLevel() );
    } else {
      // Set the default log level
      wLoglevel.select( ActionWorkflow.DEFAULT_LOG_LEVEL.getLevel() );
    }
    wAppendLogfile.setSelection( jobEntry.setAppendLogfile );
    wCreateParentFolder.setSelection( jobEntry.createParentFolder );
    wWaitingToFinish.setSelection( jobEntry.isWaitingToFinish() );
    wFollowingAbortRemotely.setSelection( jobEntry.isFollowingAbortRemotely() );
    wExpandRemote.setSelection( jobEntry.isExpandingRemoteJob() );

    List<String> runConfigurations = new ArrayList<>();
    try {
      ExtensionPointHandler
        .callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopUiRunConfiguration.id,
          new Object[] { runConfigurations, WorkflowMeta.XML_TAG } );
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

  @VisibleForTesting
  protected void getInfo( ActionWorkflow jej ) {
    String jobPath = getPath();
    jej.setName( getName() );
    jej.setFileName( jobPath );
    jej.setDirectory( null );
    jej.setWorkflowName( null );

    // Do the parameters
    int nritems = wParameters.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      if ( param != null && param.length() != 0 ) {
        nr++;
      }
    }
    jej.parameters = new String[ nr ];
    jej.parameterFieldNames = new String[ nr ];
    jej.parameterValues = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      String fieldName = wParameters.getNonEmpty( i ).getText( 2 );
      String value = wParameters.getNonEmpty( i ).getText( 3 );

      jej.parameters[ nr ] = param;

      if ( !Utils.isEmpty( Const.trim( fieldName ) ) ) {
        jej.parameterFieldNames[ nr ] = fieldName;
      } else {
        jej.parameterFieldNames[ nr ] = "";
      }

      if ( !Utils.isEmpty( Const.trim( value ) ) ) {
        jej.parameterValues[ nr ] = value;
      } else {
        jej.parameterValues[ nr ] = "";
      }

      nr++;
    }
    jej.setPassingAllParameters( wPassParams.getSelection() );

    jej.setLogfile = wSetLogfile.getSelection();
    jej.addDate = wAddDate.getSelection();
    jej.addTime = wAddTime.getSelection();
    jej.logfile = wLogfile.getText();
    jej.logext = wLogext.getText();
    if ( wLoglevel.getSelectionIndex() >= 0 ) {
      jej.logFileLevel = LogLevel.values()[ wLoglevel.getSelectionIndex() ];
    } else {
      jej.logFileLevel = LogLevel.BASIC;
    }
    jej.paramsFromPrevious = wPrevToParams.getSelection();
    jej.execPerRow = wEveryRow.getSelection();
    jej.setPassingExport( wPassExport.getSelection() );
    jej.setAppendLogfile = wAppendLogfile.getSelection();
    jej.setWaitingToFinish( wWaitingToFinish.getSelection() );
    jej.createParentFolder = wCreateParentFolder.getSelection();
    jej.setFollowingAbortRemotely( wFollowingAbortRemotely.getSelection() );
    jej.setExpandingRemoteJob( wExpandRemote.getSelection() );
    jej.setRunConfiguration( wRunConfiguration.getText() );

    WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
    executionConfiguration.setRunConfiguration( jej.getRunConfiguration() );
    try {
      ExtensionPointHandler.callExtensionPoint( jobEntry.getLogChannel(), HopExtensionPoint.HopUiPipelineBeforeStart.id,
        new Object[] { executionConfiguration, workflowMeta, workflowMeta, null } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    try {
      ExtensionPointHandler.callExtensionPoint( jobEntry.getLogChannel(), HopExtensionPoint.JobEntryPipelineSave.id,
        new Object[] { workflowMeta, jej.getRunConfiguration() } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    if ( executionConfiguration.getRemoteServer() != null ) {
      jej.setRemoteSlaveServerName( executionConfiguration.getRemoteServer().getName() );
    }
  }

  public void ok() {
    if ( Utils.isEmpty( getName() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    getInfo( jobEntry );
    jobEntry.setChanged();
    dispose();
  }

  @VisibleForTesting
  protected String getName() {
    return wName.getText();
  }

  @VisibleForTesting
  protected String getPath() {
    return wPath.getText();
  }

}
