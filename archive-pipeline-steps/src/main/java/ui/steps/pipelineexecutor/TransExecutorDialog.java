/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.pipeline.steps.pipelineexecutor;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDialogInterface;
import org.apache.hop.pipeline.steps.pipelineexecutor.PipelineExecutorMeta;
import org.apache.hop.pipeline.steps.pipelineexecutor.PipelineExecutorParameters;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.pipeline.step.BaseStepDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.io.IOException;
import java.util.Arrays;

public class PipelineExecutorDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = PipelineExecutorMeta.class; // for i18n purposes, needed by Translator!!

  private static int FIELD_DESCRIPTION = 1;
  private static int FIELD_NAME = 2;

  private PipelineExecutorMeta pipelineExecutorMeta;

  private Label wlPath;
  private TextVar wPath;

  private Button wbBrowse;

  private CTabFolder wTabFolder;

  private PipelineMeta executorPipelineMeta = null;

  protected boolean jobModified;

  private ModifyListener lsMod;

  private Button wInheritAll;

  private TableView wPipelineExecutorParameters;

  private Label wlGroupSize;
  private TextVar wGroupSize;
  private Label wlGroupField;
  private CCombo wGroupField;
  private Label wlGroupTime;
  private TextVar wGroupTime;

  private Label wlExecutionResultTarget;
  private CCombo wExecutionResultTarget;
  private TableItem tiExecutionTimeField;
  private TableItem tiExecutionResultField;
  private TableItem tiExecutionNrErrorsField;
  private TableItem tiExecutionLinesReadField;
  private TableItem tiExecutionLinesWrittenField;
  private TableItem tiExecutionLinesInputField;
  private TableItem tiExecutionLinesOutputField;
  private TableItem tiExecutionLinesRejectedField;
  private TableItem tiExecutionLinesUpdatedField;
  private TableItem tiExecutionLinesDeletedField;
  private TableItem tiExecutionFilesRetrievedField;
  private TableItem tiExecutionExitStatusField;
  private TableItem tiExecutionLogTextField;
  private TableItem tiExecutionLogChannelIdField;

  private String executorOutputStep;

  private ColumnInfo[] parameterColumns;

  private Label wlResultFilesTarget;

  private CCombo wResultFilesTarget;

  private Label wlResultFileNameField;

  private TextVar wResultFileNameField;

  private Label wlResultRowsTarget;

  private CCombo wOutputRowsSource;

  private Label wlOutputFields;

  private TableView wOutputFields;

  private Button wGetParameters;

  public PipelineExecutorDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    pipelineExecutorMeta = (PipelineExecutorMeta) in;
    jobModified = false;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, pipelineExecutorMeta );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        pipelineExecutorMeta.setChanged();
        setFlags();
      }
    };
    changed = pipelineExecutorMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, 0 );
    wlStepname.setLayoutData( fdlStepname );

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.width = 250;
    fdStepname.left = new FormAttachment( 0, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 5 );
    wStepname.setLayoutData( fdStepname );

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    spacer.setLayoutData( fdSpacer );

    wlPath = new Label( shell, SWT.LEFT );
    props.setLook( wlPath );
    wlPath.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Pipeline.Label" ) );
    FormData fdlTransformation = new FormData();
    fdlTransformation.left = new FormAttachment( 0, 0 );
    fdlTransformation.top = new FormAttachment( spacer, 20 );
    fdlTransformation.right = new FormAttachment( 50, 0 );
    wlPath.setLayoutData( fdlTransformation );

    wPath = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPath );
    FormData fdTransformation = new FormData();
    fdTransformation.left = new FormAttachment( 0, 0 );
    fdTransformation.top = new FormAttachment( wlPath, 5 );
    fdTransformation.width = 350;
    wPath.setLayoutData( fdTransformation );

    wbBrowse = new Button( shell, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Browse.Label" ) );
    FormData fdBrowse = new FormData();
    fdBrowse.left = new FormAttachment( wPath, 5 );
    fdBrowse.top = new FormAttachment( wlPath, Const.isOSX() ? 0 : 5 );
    wbBrowse.setLayoutData( fdBrowse );

    wbBrowse.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        selectFilePipeline();
      }
    } );

    //
    // Add a tab folder for the parameters and various input and output
    // streams
    //
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    wTabFolder.setUnselectedCloseVisible( true );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100, 0 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment( wCancel, -5 );
    fdOk.bottom = new FormAttachment( 100, 0 );
    wOK.setLayoutData( fdOk );

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment( 0, 0 );
    fdhSpacer.bottom = new FormAttachment( wCancel, -15 );
    fdhSpacer.right = new FormAttachment( 100, 0 );
    hSpacer.setLayoutData( fdhSpacer );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wPath, 20 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( hSpacer, -15 );
    wTabFolder.setLayoutData( fdTabFolder );

    // Add the tabs...
    //
    addParametersTab();
    addExecutionResultTab();
    addRowGroupTab();
    addResultRowsTab();
    addResultFilesTab();

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wPath.addSelectionListener( lsDef );
    wResultFileNameField.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize( shell, 620, 675 );

    getData();
    pipelineExecutorMeta.setChanged( changed );
    wTabFolder.setSelection( 0 );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected Image getImage() {
    return SwtSvgImageUtil
      .getImage( shell.getDisplay(), getClass().getClassLoader(), "TRNEx.svg", ConstUI.LARGE_ICON_SIZE,
        ConstUI.LARGE_ICON_SIZE );
  }

  private void selectFilePipeline() {
    String curFile = pipelineMeta.environmentSubstitute( wPath.getText() );

    FileObject root = null;

    String parentFolder = null;
    try {
      parentFolder =
        HopVFS.getFileObject( pipelineMeta.environmentSubstitute( pipelineMeta.getFilename() ) ).getParent().toString();
    } catch ( Exception e ) {
      // Take no action
    }

    try {
      root = HopVFS.getFileObject( curFile != null ? curFile : Const.getUserHomeDirectory() );

      VfsFileChooserDialog vfsFileChooser = HopUi.getInstance().getVfsFileChooserDialog( root.getParent(), root );
      FileObject file =
        vfsFileChooser.open(
          shell, null, Const.STRING_PIPELINE_FILTER_EXT, Const.getTransformationFilterNames(),
          VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE );
      if ( file == null ) {
        return;
      }
      String fileName = file.getName().toString();
      if ( fileName != null ) {
        loadPipelineFile( fileName );
        if ( parentFolder != null && fileName.startsWith( parentFolder ) ) {
          fileName = fileName.replace( parentFolder, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}" );
        }
        wPath.setText( fileName );
      }
    } catch ( IOException | HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingPipeline.DialogTitle" ),
        BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingPipeline.DialogMessage" ), e );
    }
  }

  private void loadPipelineFile( String fname ) throws HopException {
    executorPipelineMeta = new PipelineMeta( pipelineMeta.environmentSubstitute( fname ) );
    executorPipelineMeta.clearChanged();
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  void loadPipeline() throws HopException {
    String filename = wPath.getText();
    if ( Utils.isEmpty( filename ) ) {
      return;
    }
    if ( !filename.endsWith( ".hpl" ) ) {
      filename = filename + ".hpl";
      wPath.setText( filename );
    }
    loadPipelineFile( filename );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wPath.setText( Const.NVL( pipelineExecutorMeta.getFileName(), "" ) );

    // TODO: throw in a separate thread.
    //
    try {
      String[] prevSteps = pipelineMeta.getStepNames();
      Arrays.sort( prevSteps );
      wExecutionResultTarget.setItems( prevSteps );
      wResultFilesTarget.setItems( prevSteps );
      wOutputRowsSource.setItems( prevSteps );

      String[] inputFields = pipelineMeta.getPrevStepFields( stepMeta ).getFieldNames();
      parameterColumns[ 1 ].setComboValues( inputFields );
      wGroupField.setItems( inputFields );
    } catch ( Exception e ) {
      log.logError( "couldn't get previous step list", e );
    }

    wGroupSize.setText( Const.NVL( pipelineExecutorMeta.getGroupSize(), "" ) );
    wGroupTime.setText( Const.NVL( pipelineExecutorMeta.getGroupTime(), "" ) );
    wGroupField.setText( Const.NVL( pipelineExecutorMeta.getGroupField(), "" ) );

    wExecutionResultTarget.setText( pipelineExecutorMeta.getExecutionResultTargetStepMeta() == null ? ""
      : pipelineExecutorMeta.getExecutionResultTargetStepMeta().getName() );
    tiExecutionTimeField.setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionTimeField(), "" ) );
    tiExecutionResultField.setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionResultField(), "" ) );
    tiExecutionNrErrorsField.setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionNrErrorsField(), "" ) );
    tiExecutionLinesReadField.setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLinesReadField(), "" ) );
    tiExecutionLinesWrittenField
      .setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLinesWrittenField(), "" ) );
    tiExecutionLinesInputField.setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLinesInputField(), "" ) );
    tiExecutionLinesOutputField
      .setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLinesOutputField(), "" ) );
    tiExecutionLinesRejectedField
      .setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLinesRejectedField(), "" ) );
    tiExecutionLinesUpdatedField
      .setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLinesUpdatedField(), "" ) );
    tiExecutionLinesDeletedField
      .setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLinesDeletedField(), "" ) );
    tiExecutionFilesRetrievedField
      .setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionFilesRetrievedField(), "" ) );
    tiExecutionExitStatusField.setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionExitStatusField(), "" ) );
    tiExecutionLogTextField.setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLogTextField(), "" ) );
    tiExecutionLogChannelIdField
      .setText( FIELD_NAME, Const.NVL( pipelineExecutorMeta.getExecutionLogChannelIdField(), "" ) );

    if ( pipelineExecutorMeta.getExecutorsOutputStepMeta() != null ) {
      executorOutputStep = pipelineExecutorMeta.getExecutorsOutputStepMeta().getName();
    }

    // result files
    //
    wResultFilesTarget.setText( pipelineExecutorMeta.getResultFilesTargetStepMeta() == null ? "" : pipelineExecutorMeta
      .getResultFilesTargetStepMeta().getName() );
    wResultFileNameField.setText( Const.NVL( pipelineExecutorMeta.getResultFilesFileNameField(), "" ) );

    // Result rows
    //
    wOutputRowsSource.setText( pipelineExecutorMeta.getOutputRowsSourceStepMeta() == null ? "" : pipelineExecutorMeta
      .getOutputRowsSourceStepMeta().getName() );
    for ( int i = 0; i < pipelineExecutorMeta.getOutputRowsField().length; i++ ) {
      TableItem item = new TableItem( wOutputFields.table, SWT.NONE );
      item.setText( 1, Const.NVL( pipelineExecutorMeta.getOutputRowsField()[ i ], "" ) );
      item.setText( 2, ValueMetaFactory.getValueMetaName( pipelineExecutorMeta.getOutputRowsType()[ i ] ) );
      int length = pipelineExecutorMeta.getOutputRowsLength()[ i ];
      item.setText( 3, length < 0 ? "" : Integer.toString( length ) );
      int precision = pipelineExecutorMeta.getOutputRowsPrecision()[ i ];
      item.setText( 4, precision < 0 ? "" : Integer.toString( precision ) );
    }
    wOutputFields.removeEmptyRows();
    wOutputFields.setRowNums();
    wOutputFields.optWidth( true );

    wTabFolder.setSelection( 0 );

    try {
      loadPipeline();
    } catch ( Throwable t ) {
      // Ignore errors
    }

    setFlags();

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void addParametersTab() {
    CTabItem wParametersTab = new CTabItem( wTabFolder, SWT.NONE );
    wParametersTab.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Parameters.Title" ) );
    wParametersTab.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Parameters.Tooltip" ) );

    Composite wParametersComposite = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wParametersComposite );

    FormLayout parameterTabLayout = new FormLayout();
    parameterTabLayout.marginWidth = 15;
    parameterTabLayout.marginHeight = 15;
    wParametersComposite.setLayout( parameterTabLayout );

    // Add a button: get parameters
    //
    wGetParameters = new Button( wParametersComposite, SWT.PUSH );
    wGetParameters.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Parameters.GetParameters" ) );
    props.setLook( wGetParameters );
    FormData fdGetParameters = new FormData();
    fdGetParameters.bottom = new FormAttachment( 100, 0 );
    fdGetParameters.right = new FormAttachment( 100, 0 );
    wGetParameters.setLayoutData( fdGetParameters );
    wGetParameters.setSelection( pipelineExecutorMeta.getParameters().isInheritingAllVariables() );
    wGetParameters.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getParametersFromPipeline( null ); // null = force reload of data on disk
      }
    } );

    // Now add a table view with the 3 columns to specify: variable name, input field & optional static input
    //
    parameterColumns =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorDialog.Parameters.column.Variable" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorDialog.Parameters.column.Field" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {}, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorDialog.Parameters.column.Input" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ), };
    parameterColumns[ 1 ].setUsingVariables( true );

    PipelineExecutorParameters parameters = pipelineExecutorMeta.getParameters();
    wPipelineExecutorParameters =
      new TableView( pipelineMeta, wParametersComposite, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, parameterColumns,
        parameters.getVariable().length, false, lsMod, props, false );
    props.setLook( wPipelineExecutorParameters );
    FormData fdPipelineExecutors = new FormData();
    fdPipelineExecutors.left = new FormAttachment( 0, 0 );
    fdPipelineExecutors.right = new FormAttachment( 100, 0 );
    fdPipelineExecutors.top = new FormAttachment( 0, 0 );
    fdPipelineExecutors.bottom = new FormAttachment( wGetParameters, -10 );
    wPipelineExecutorParameters.setLayoutData( fdPipelineExecutors );
    wPipelineExecutorParameters.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 33, 33, 33 ) );

    for ( int i = 0; i < parameters.getVariable().length; i++ ) {
      TableItem tableItem = wPipelineExecutorParameters.table.getItem( i );
      tableItem.setText( 1, Const.NVL( parameters.getVariable()[ i ], "" ) );
      tableItem.setText( 2, Const.NVL( parameters.getField()[ i ], "" ) );
      tableItem.setText( 3, Const.NVL( parameters.getInput()[ i ], "" ) );
    }
    wPipelineExecutorParameters.setRowNums();
    wPipelineExecutorParameters.optWidth( true );

    // Add a checkbox: inherit all variables...
    //
    wInheritAll = new Button( wParametersComposite, SWT.CHECK );
    wInheritAll.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.Parameters.InheritAll" ) );
    props.setLook( wInheritAll );
    FormData fdInheritAll = new FormData();
    fdInheritAll.top = new FormAttachment( wPipelineExecutorParameters, 15 );
    fdInheritAll.left = new FormAttachment( 0, 0 );
    wInheritAll.setLayoutData( fdInheritAll );
    wInheritAll.setSelection( pipelineExecutorMeta.getParameters().isInheritingAllVariables() );

    FormData fdParametersComposite = new FormData();
    fdParametersComposite.left = new FormAttachment( 0, 0 );
    fdParametersComposite.top = new FormAttachment( 0, 0 );
    fdParametersComposite.right = new FormAttachment( 100, 0 );
    fdParametersComposite.bottom = new FormAttachment( 100, 0 );
    wParametersComposite.setLayoutData( fdParametersComposite );

    wParametersComposite.layout();
    wParametersTab.setControl( wParametersComposite );
  }

  protected void getParametersFromPipeline( PipelineMeta inputPipelineMeta ) {
    try {
      // Load the job in executorPipelineMeta
      //
      if ( inputPipelineMeta == null ) {
        loadPipeline();
        inputPipelineMeta = executorPipelineMeta;
      }

      String[] parameters = inputPipelineMeta.listParameters();
      for ( int i = 0; i < parameters.length; i++ ) {
        String name = parameters[ i ];
        String desc = inputPipelineMeta.getParameterDescription( name );

        TableItem item = new TableItem( wPipelineExecutorParameters.table, SWT.NONE );
        item.setText( 1, Const.NVL( name, "" ) );
        String str = inputPipelineMeta.getParameterDefault( name );
        str = ( str != null ? str : ( desc != null ? desc : "" ) );
        item.setText( 3, Const.NVL( str, "" ) );
      }
      wPipelineExecutorParameters.removeEmptyRows();
      wPipelineExecutorParameters.setRowNums();
      wPipelineExecutorParameters.optWidth( true );

    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingSpecifiedPipeline.Title" ),
        BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingSpecifiedPipeline.Message" ), e );
    }

  }

  private void addRowGroupTab() {

    final CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.RowGroup.Title" ) );
    wTab.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutorDialog.RowGroup.Tooltip" ) );

    Composite wInputComposite = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wInputComposite );

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout( tabLayout );

    // Group size
    //
    wlGroupSize = new Label( wInputComposite, SWT.RIGHT );
    props.setLook( wlGroupSize );
    wlGroupSize.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.GroupSize.Label" ) );
    FormData fdlGroupSize = new FormData();
    fdlGroupSize.top = new FormAttachment( 0, 0 );
    fdlGroupSize.left = new FormAttachment( 0, 0 );
    wlGroupSize.setLayoutData( fdlGroupSize );

    wGroupSize = new TextVar( pipelineMeta, wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGroupSize );
    wGroupSize.addModifyListener( lsMod );
    FormData fdGroupSize = new FormData();
    fdGroupSize.width = 250;
    fdGroupSize.top = new FormAttachment( wlGroupSize, 5 );
    fdGroupSize.left = new FormAttachment( 0, 0 );
    wGroupSize.setLayoutData( fdGroupSize );

    // Group field
    //
    wlGroupField = new Label( wInputComposite, SWT.RIGHT );
    props.setLook( wlGroupField );
    wlGroupField.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.GroupField.Label" ) );
    FormData fdlGroupField = new FormData();
    fdlGroupField.top = new FormAttachment( wGroupSize, 10 );
    fdlGroupField.left = new FormAttachment( 0, 0 );
    wlGroupField.setLayoutData( fdlGroupField );

    wGroupField = new CCombo( wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGroupField );
    wGroupField.addModifyListener( lsMod );
    FormData fdGroupField = new FormData();
    fdGroupField.width = 250;
    fdGroupField.top = new FormAttachment( wlGroupField, 5 );
    fdGroupField.left = new FormAttachment( 0, 0 );
    wGroupField.setLayoutData( fdGroupField );

    // Group time
    //
    wlGroupTime = new Label( wInputComposite, SWT.RIGHT );
    props.setLook( wlGroupTime );
    wlGroupTime.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.GroupTime.Label" ) );
    FormData fdlGroupTime = new FormData();
    fdlGroupTime.top = new FormAttachment( wGroupField, 10 );
    fdlGroupTime.left = new FormAttachment( 0, 0 ); // First one in the left
    wlGroupTime.setLayoutData( fdlGroupTime );

    wGroupTime = new TextVar( pipelineMeta, wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGroupTime );
    wGroupTime.addModifyListener( lsMod );
    FormData fdGroupTime = new FormData();
    fdGroupTime.width = 250;
    fdGroupTime.top = new FormAttachment( wlGroupTime, 5 );
    fdGroupTime.left = new FormAttachment( 0, 0 );
    wGroupTime.setLayoutData( fdGroupTime );

    wTab.setControl( wInputComposite );
    wTabFolder.setSelection( wTab );
  }

  private void addExecutionResultTab() {

    final CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionResults.Title" ) );
    wTab.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionResults.Tooltip" ) );

    ScrolledComposite scrolledComposite = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    scrolledComposite.setLayout( new FillLayout() );

    Composite wInputComposite = new Composite( scrolledComposite, SWT.NONE );
    props.setLook( wInputComposite );

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout( tabLayout );

    wlExecutionResultTarget = new Label( wInputComposite, SWT.RIGHT );
    props.setLook( wlExecutionResultTarget );
    wlExecutionResultTarget.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionResultTarget.Label" ) );
    FormData fdlExecutionResultTarget = new FormData();
    fdlExecutionResultTarget.top = new FormAttachment( 0, 0 );
    fdlExecutionResultTarget.left = new FormAttachment( 0, 0 ); // First one in the left
    wlExecutionResultTarget.setLayoutData( fdlExecutionResultTarget );

    wExecutionResultTarget = new CCombo( wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExecutionResultTarget );
    wExecutionResultTarget.addModifyListener( lsMod );
    FormData fdExecutionResultTarget = new FormData();
    fdExecutionResultTarget.width = 250;
    fdExecutionResultTarget.top = new FormAttachment( wlExecutionResultTarget, 5 );
    fdExecutionResultTarget.left = new FormAttachment( 0, 0 ); // To the right
    wExecutionResultTarget.setLayoutData( fdExecutionResultTarget );

    ColumnInfo[] executionResultColumns =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorMeta.ExecutionResults.FieldDescription.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorMeta.ExecutionResults.FieldName.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false )
      };
    executionResultColumns[ 1 ].setUsingVariables( true );

    TableView wExectionResults =
      new TableView( pipelineMeta, wInputComposite, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, executionResultColumns,
        14, false, lsMod, props, false );
    props.setLook( wExectionResults );
    FormData fdExecutionResults = new FormData();
    fdExecutionResults.left = new FormAttachment( 0 );
    fdExecutionResults.right = new FormAttachment( 100 );
    fdExecutionResults.top = new FormAttachment( wExecutionResultTarget, 10 );
    fdExecutionResults.bottom = new FormAttachment( 100 );
    wExectionResults.setLayoutData( fdExecutionResults );
    wExectionResults.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 50, 50 ) );

    int index = 0;
    tiExecutionTimeField = wExectionResults.table.getItem( index++ );
    tiExecutionResultField = wExectionResults.table.getItem( index++ );
    tiExecutionNrErrorsField = wExectionResults.table.getItem( index++ );
    tiExecutionLinesReadField = wExectionResults.table.getItem( index++ );
    tiExecutionLinesWrittenField = wExectionResults.table.getItem( index++ );
    tiExecutionLinesInputField = wExectionResults.table.getItem( index++ );
    tiExecutionLinesOutputField = wExectionResults.table.getItem( index++ );
    tiExecutionLinesRejectedField = wExectionResults.table.getItem( index++ );
    tiExecutionLinesUpdatedField = wExectionResults.table.getItem( index++ );
    tiExecutionLinesDeletedField = wExectionResults.table.getItem( index++ );
    tiExecutionFilesRetrievedField = wExectionResults.table.getItem( index++ );
    tiExecutionExitStatusField = wExectionResults.table.getItem( index++ );
    tiExecutionLogTextField = wExectionResults.table.getItem( index++ );
    tiExecutionLogChannelIdField = wExectionResults.table.getItem( index++ );

    tiExecutionTimeField
      .setText( FIELD_DESCRIPTION, BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionTimeField.Label" ) );
    tiExecutionResultField
      .setText( FIELD_DESCRIPTION, BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionResultField.Label" ) );
    tiExecutionNrErrorsField
      .setText( FIELD_DESCRIPTION, BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionNrErrorsField.Label" ) );
    tiExecutionLinesReadField
      .setText( FIELD_DESCRIPTION, BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLinesReadField.Label" ) );
    tiExecutionLinesWrittenField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLinesWrittenField.Label" ) );
    tiExecutionLinesInputField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLinesInputField.Label" ) );
    tiExecutionLinesOutputField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLinesOutputField.Label" ) );
    tiExecutionLinesRejectedField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLinesRejectedField.Label" ) );
    tiExecutionLinesUpdatedField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLinesUpdatedField.Label" ) );
    tiExecutionLinesDeletedField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLinesDeletedField.Label" ) );
    tiExecutionFilesRetrievedField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionFilesRetrievedField.Label" ) );
    tiExecutionExitStatusField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionExitStatusField.Label" ) );
    tiExecutionLogTextField
      .setText( FIELD_DESCRIPTION, BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLogTextField.Label" ) );
    tiExecutionLogChannelIdField.setText( FIELD_DESCRIPTION,
      BaseMessages.getString( PKG, "PipelineExecutorDialog.ExecutionLogChannelIdField.Label" ) );

    wPipelineExecutorParameters.setRowNums();
    wPipelineExecutorParameters.optWidth( true );

    wInputComposite.pack();
    Rectangle bounds = wInputComposite.getBounds();

    scrolledComposite.setContent( wInputComposite );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setMinWidth( bounds.width );
    scrolledComposite.setMinHeight( bounds.height );

    wTab.setControl( scrolledComposite );
    wTabFolder.setSelection( wTab );
  }

  private void addResultFilesTab() {

    final CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ResultFiles.Title" ) );
    wTab.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ResultFiles.Tooltip" ) );

    ScrolledComposite scrolledComposite = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    scrolledComposite.setLayout( new FillLayout() );

    Composite wInputComposite = new Composite( scrolledComposite, SWT.NONE );
    props.setLook( wInputComposite );

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout( tabLayout );

    wlResultFilesTarget = new Label( wInputComposite, SWT.RIGHT );
    props.setLook( wlResultFilesTarget );
    wlResultFilesTarget.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ResultFilesTarget.Label" ) );
    FormData fdlResultFilesTarget = new FormData();
    fdlResultFilesTarget.top = new FormAttachment( 0, 0 );
    fdlResultFilesTarget.left = new FormAttachment( 0, 0 ); // First one in the left
    wlResultFilesTarget.setLayoutData( fdlResultFilesTarget );

    wResultFilesTarget = new CCombo( wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResultFilesTarget );
    wResultFilesTarget.addModifyListener( lsMod );
    FormData fdResultFilesTarget = new FormData();
    fdResultFilesTarget.width = 250;
    fdResultFilesTarget.top = new FormAttachment( wlResultFilesTarget, 5 );
    fdResultFilesTarget.left = new FormAttachment( 0, 0 ); // To the right
    wResultFilesTarget.setLayoutData( fdResultFilesTarget );

    // ResultFileNameField
    //
    wlResultFileNameField = new Label( wInputComposite, SWT.RIGHT );
    props.setLook( wlResultFileNameField );
    wlResultFileNameField.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ResultFileNameField.Label" ) );
    FormData fdlResultFileNameField = new FormData();
    fdlResultFileNameField.top = new FormAttachment( wResultFilesTarget, 10 );
    fdlResultFileNameField.left = new FormAttachment( 0, 0 ); // First one in the left
    wlResultFileNameField.setLayoutData( fdlResultFileNameField );

    wResultFileNameField = new TextVar( pipelineMeta, wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResultFileNameField );
    wResultFileNameField.addModifyListener( lsMod );
    FormData fdResultFileNameField = new FormData();
    fdResultFileNameField.width = 250;
    fdResultFileNameField.top = new FormAttachment( wlResultFileNameField, 5 );
    fdResultFileNameField.left = new FormAttachment( 0, 0 ); // To the right
    wResultFileNameField.setLayoutData( fdResultFileNameField );

    wInputComposite.pack();
    Rectangle bounds = wInputComposite.getBounds();

    scrolledComposite.setContent( wInputComposite );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setMinWidth( bounds.width );
    scrolledComposite.setMinHeight( bounds.height );

    wTab.setControl( scrolledComposite );
    wTabFolder.setSelection( wTab );
  }

  private void addResultRowsTab() {

    final CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ResultRows.Title" ) );
    wTab.setToolTipText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ResultRows.Tooltip" ) );

    ScrolledComposite scrolledComposite = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    scrolledComposite.setLayout( new FillLayout() );

    Composite wInputComposite = new Composite( scrolledComposite, SWT.NONE );
    props.setLook( wInputComposite );

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout( tabLayout );

    wlResultRowsTarget = new Label( wInputComposite, SWT.RIGHT );
    props.setLook( wlResultRowsTarget );
    wlResultRowsTarget.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.OutputRowsSource.Label" ) );
    FormData fdlResultRowsTarget = new FormData();
    fdlResultRowsTarget.top = new FormAttachment( 0, 0 );
    fdlResultRowsTarget.left = new FormAttachment( 0, 0 ); // First one in the left
    wlResultRowsTarget.setLayoutData( fdlResultRowsTarget );

    wOutputRowsSource = new CCombo( wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputRowsSource );
    wOutputRowsSource.addModifyListener( lsMod );
    FormData fdResultRowsTarget = new FormData();
    fdResultRowsTarget.width = 250;
    fdResultRowsTarget.top = new FormAttachment( wlResultRowsTarget, 5 );
    fdResultRowsTarget.left = new FormAttachment( 0, 0 ); // To the right
    wOutputRowsSource.setLayoutData( fdResultRowsTarget );

    wlOutputFields = new Label( wInputComposite, SWT.NONE );
    wlOutputFields.setText( BaseMessages.getString( PKG, "PipelineExecutorDialog.ResultFields.Label" ) );
    props.setLook( wlOutputFields );
    FormData fdlResultFields = new FormData();
    fdlResultFields.left = new FormAttachment( 0, 0 );
    fdlResultFields.top = new FormAttachment( wOutputRowsSource, 10 );
    wlOutputFields.setLayoutData( fdlResultFields );

    int nrRows = ( pipelineExecutorMeta.getOutputRowsField() != null ? pipelineExecutorMeta.getOutputRowsField().length : 1 );

    ColumnInfo[] ciResultFields =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorDialog.ColumnInfo.Field" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorDialog.ColumnInfo.Type" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorDialog.ColumnInfo.Length" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "PipelineExecutorDialog.ColumnInfo.Precision" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    wOutputFields =
      new TableView( pipelineMeta, wInputComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL
        | SWT.H_SCROLL, ciResultFields, nrRows, false, lsMod, props, false );

    FormData fdResultFields = new FormData();
    fdResultFields.left = new FormAttachment( 0, 0 );
    fdResultFields.top = new FormAttachment( wlOutputFields, 5 );
    fdResultFields.right = new FormAttachment( 100, 0 );
    fdResultFields.bottom = new FormAttachment( 100, 0 );
    wOutputFields.setLayoutData( fdResultFields );
    wOutputFields.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 25, 25, 25, 25 ) );

    wInputComposite.pack();
    Rectangle bounds = wInputComposite.getBounds();

    scrolledComposite.setContent( wInputComposite );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setMinWidth( bounds.width );
    scrolledComposite.setMinHeight( bounds.height );

    wTab.setControl( scrolledComposite );
    wTabFolder.setSelection( wTab );
  }

  private void setFlags() {
    // Enable/disable fields...
    //
    if ( wlGroupSize == null || wlGroupSize == null || wlGroupField == null || wGroupField == null
      || wlGroupTime == null || wGroupTime == null ) {
      return;
    }
    boolean enableSize = Const.toInt( pipelineMeta.environmentSubstitute( wGroupSize.getText() ), -1 ) >= 0;
    boolean enableField = !Utils.isEmpty( wGroupField.getText() );
    // boolean enableTime = Const.toInt(pipelineMeta.environmentSubstitute(wGroupTime.getText()), -1)>0;

    wlGroupSize.setEnabled( true );
    wGroupSize.setEnabled( true );
    wlGroupField.setEnabled( !enableSize );
    wGroupField.setEnabled( !enableSize );
    wlGroupTime.setEnabled( !enableSize && !enableField );
    wGroupTime.setEnabled( !enableSize && !enableField );
  }

  private void cancel() {
    stepname = null;
    pipelineExecutorMeta.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    try {
      loadPipeline();
    } catch ( HopException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingSpecifiedPipeline.Title" ),
        BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingSpecifiedPipeline.Message" ), e );
    }

    pipelineExecutorMeta.setFileName( wPath.getText() );

    // Load the information on the tabs, optionally do some
    // verifications...
    //
    collectInformation();

    // Set the input steps for input mappings
    pipelineExecutorMeta.searchInfoAndTargetSteps( pipelineMeta.getSteps() );

    pipelineExecutorMeta.setChanged( true );

    dispose();
  }

  private void collectInformation() {
    // The parameters...
    //
    PipelineExecutorParameters parameters = pipelineExecutorMeta.getParameters();

    int nrLines = wPipelineExecutorParameters.nrNonEmpty();
    String[] variables = new String[ nrLines ];
    String[] fields = new String[ nrLines ];
    String[] input = new String[ nrLines ];
    parameters.setVariable( variables );
    parameters.setField( fields );
    parameters.setInput( input );
    for ( int i = 0; i < nrLines; i++ ) {
      TableItem item = wPipelineExecutorParameters.getNonEmpty( i );
      variables[ i ] = item.getText( 1 );
      fields[ i ] = item.getText( 2 );
      input[ i ] = item.getText( 3 );
    }
    parameters.setInheritingAllVariables( wInheritAll.getSelection() );

    // The group definition
    //
    pipelineExecutorMeta.setGroupSize( wGroupSize.getText() );
    pipelineExecutorMeta.setGroupField( wGroupField.getText() );
    pipelineExecutorMeta.setGroupTime( wGroupTime.getText() );

    pipelineExecutorMeta.setExecutionResultTargetStep( wExecutionResultTarget.getText() );
    pipelineExecutorMeta.setExecutionResultTargetStepMeta( pipelineMeta.findStep( wExecutionResultTarget.getText() ) );
    pipelineExecutorMeta.setExecutionTimeField( tiExecutionTimeField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionResultField( tiExecutionResultField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionNrErrorsField( tiExecutionNrErrorsField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLinesReadField( tiExecutionLinesReadField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLinesWrittenField( tiExecutionLinesWrittenField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLinesInputField( tiExecutionLinesInputField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLinesOutputField( tiExecutionLinesOutputField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLinesRejectedField( tiExecutionLinesRejectedField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLinesUpdatedField( tiExecutionLinesUpdatedField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLinesDeletedField( tiExecutionLinesDeletedField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionFilesRetrievedField( tiExecutionFilesRetrievedField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionExitStatusField( tiExecutionExitStatusField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLogTextField( tiExecutionLogTextField.getText( FIELD_NAME ) );
    pipelineExecutorMeta.setExecutionLogChannelIdField( tiExecutionLogChannelIdField.getText( FIELD_NAME ) );

    pipelineExecutorMeta.setResultFilesTargetStep( wResultFilesTarget.getText() );
    pipelineExecutorMeta.setResultFilesTargetStepMeta( pipelineMeta.findStep( wResultFilesTarget.getText() ) );
    pipelineExecutorMeta.setResultFilesFileNameField( wResultFileNameField.getText() );

    if ( !Utils.isEmpty( executorOutputStep ) ) {
      pipelineExecutorMeta.setExecutorsOutputStep( executorOutputStep );
      pipelineExecutorMeta.setExecutorsOutputStepMeta( pipelineMeta.findStep( executorOutputStep ) );
    }

    // Result row info
    //
    pipelineExecutorMeta.setOutputRowsSourceStep( wOutputRowsSource.getText() );
    pipelineExecutorMeta.setOutputRowsSourceStepMeta( pipelineMeta.findStep( wOutputRowsSource.getText() ) );
    int nrFields = wOutputFields.nrNonEmpty();
    pipelineExecutorMeta.setOutputRowsField( new String[ nrFields ] );
    pipelineExecutorMeta.setOutputRowsType( new int[ nrFields ] );
    pipelineExecutorMeta.setOutputRowsLength( new int[ nrFields ] );
    pipelineExecutorMeta.setOutputRowsPrecision( new int[ nrFields ] );

    // CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wOutputFields.getNonEmpty( i );
      pipelineExecutorMeta.getOutputRowsField()[ i ] = item.getText( 1 );
      pipelineExecutorMeta.getOutputRowsType()[ i ] = ValueMetaFactory.getIdForValueMeta( item.getText( 2 ) );
      pipelineExecutorMeta.getOutputRowsLength()[ i ] = Const.toInt( item.getText( 3 ), -1 );
      pipelineExecutorMeta.getOutputRowsPrecision()[ i ] = Const.toInt( item.getText( 4 ), -1 );
    }

  }
}
