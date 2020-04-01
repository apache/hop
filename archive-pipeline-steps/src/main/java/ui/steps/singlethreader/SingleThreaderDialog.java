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

package org.apache.hop.ui.pipeline.steps.singlethreader;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDialogInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.steps.singlethreader.SingleThreaderMeta;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.pipeline.step.BaseStepDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.io.IOException;

public class SingleThreaderDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = SingleThreaderMeta.class; // for i18n purposes, needed by Translator!!

  private SingleThreaderMeta singleThreaderMeta;

  private Label wlPath;
  private TextVar wPath;

  private Button wbBrowse;

  private TextVar wBatchSize;
  private TextVar wInjectStep;
  private Button wGetInjectStep;
  private TextVar wRetrieveStep;
  private Button wGetRetrieveStep;

  private TableView wParameters;

  private PipelineMeta mappingPipelineMeta = null;

  protected boolean transModified;

  private ModifyListener lsMod;

  private Button wPassParams;

  private Button wbGetParams;

  private TextVar wBatchTime;

  public SingleThreaderDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    singleThreaderMeta = (SingleThreaderMeta) in;
    transModified = false;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, singleThreaderMeta );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        singleThreaderMeta.setChanged();
      }
    };
    changed = singleThreaderMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Stepname.Label" ) );
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
    wlPath.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Pipeline.Label" ) );
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
    wbBrowse.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Browse.Label" ) );
    FormData fdBrowse = new FormData();
    fdBrowse.left = new FormAttachment( wPath, 5 );
    fdBrowse.top = new FormAttachment( wlPath, Const.isOSX() ? 0 : 5 );
    wbBrowse.setLayoutData( fdBrowse );

    wbBrowse.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        selectPipelineFile( true );
      }
    } );

    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // Options Tab Start
    CTabItem wOptionsTab = new CTabItem( wTabFolder, SWT.NONE );
    wOptionsTab.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Options.Group.Label" ) );

    Composite wOptions = new Composite( wTabFolder, SWT.SHADOW_NONE );
    props.setLook( wOptions );

    FormLayout specLayout = new FormLayout();
    specLayout.marginWidth = 15;
    specLayout.marginHeight = 15;
    wOptions.setLayout( specLayout );

    // Inject step
    //
    Label wlInjectStep = new Label( wOptions, SWT.LEFT );
    wlInjectStep.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.InjectStep.Label" ) );
    props.setLook( wlInjectStep );
    FormData fdlInjectStep = new FormData();
    fdlInjectStep.top = new FormAttachment( 0, 0 );
    fdlInjectStep.left = new FormAttachment( 0, 0 );
    wlInjectStep.setLayoutData( fdlInjectStep );

    wInjectStep = new TextVar( pipelineMeta, wOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInjectStep );
    wInjectStep.addModifyListener( lsMod );
    FormData fdInjectStep = new FormData();
    fdInjectStep.width = 250;
    fdInjectStep.left = new FormAttachment( 0, 0 );
    fdInjectStep.top = new FormAttachment( wlInjectStep, 5 );
    wInjectStep.setLayoutData( fdInjectStep );

    wGetInjectStep = new Button( wOptions, SWT.PUSH );
    wGetInjectStep.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Button.Get" ) );
    FormData fdGetInjectStep = new FormData();
    fdGetInjectStep.top = new FormAttachment( wlInjectStep, Const.isOSX() ? 0 : 5 );
    fdGetInjectStep.left = new FormAttachment( wInjectStep, 5 );
    wGetInjectStep.setLayoutData( fdGetInjectStep );
    wGetInjectStep.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        try {
          loadPipeline();
          String stepname = mappingPipelineMeta == null ? "" : Const.NVL( getInjectorStep( mappingPipelineMeta ), "" );
          wInjectStep.setText( stepname );
        } catch ( Exception e ) {
          new ErrorDialog( shell,
            BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingPipeline.DialogTitle" ),
            BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingPipeline.DialogMessage" ), e );
        }
      }
    } );

    // Retrieve step...
    //
    Label wlRetrieveStep = new Label( wOptions, SWT.LEFT );
    wlRetrieveStep.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.RetrieveStep.Label" ) );
    props.setLook( wlRetrieveStep );
    FormData fdlRetrieveStep = new FormData();
    fdlRetrieveStep.top = new FormAttachment( wInjectStep, 10 );
    fdlRetrieveStep.left = new FormAttachment( 0, 0 );
    wlRetrieveStep.setLayoutData( fdlRetrieveStep );

    wRetrieveStep = new TextVar( pipelineMeta, wOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRetrieveStep );
    wRetrieveStep.addModifyListener( lsMod );
    FormData fdRetrieveStep = new FormData();
    fdRetrieveStep.width = 250;
    fdRetrieveStep.left = new FormAttachment( 0, 0 );
    fdRetrieveStep.top = new FormAttachment( wlRetrieveStep, 5 );
    wRetrieveStep.setLayoutData( fdRetrieveStep );

    wGetRetrieveStep = new Button( wOptions, SWT.PUSH );
    wGetRetrieveStep.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Button.Get" ) );
    FormData fdGetRetrieveStep = new FormData();
    fdGetRetrieveStep.top = new FormAttachment( wlRetrieveStep, Const.isOSX() ? 0 : 5 );
    fdGetRetrieveStep.left = new FormAttachment( wRetrieveStep, 5 );
    wGetRetrieveStep.setLayoutData( fdGetRetrieveStep );
    wGetRetrieveStep.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        try {
          loadPipeline();
          if ( mappingPipelineMeta != null ) {
            String[] stepNames = mappingPipelineMeta.getStepNames();
            EnterSelectionDialog d = new EnterSelectionDialog( shell, stepNames,
              BaseMessages.getString( PKG, "SingleThreaderDialog.SelectStep.Title" ),
              BaseMessages.getString( PKG, "SingleThreaderDialog.SelectStep.Message" ) );
            String step = d.open();
            if ( step != null ) {
              wRetrieveStep.setText( step );
            }
          }
        } catch ( Exception e ) {
          new ErrorDialog( shell,
            BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingPipeline.DialogTitle" ),
            BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingPipeline.DialogMessage" ), e );
        }
      }
    } );

    // Here come the batch size, inject and retrieve fields...
    //
    Label wlBatchSize = new Label( wOptions, SWT.LEFT );
    wlBatchSize.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.BatchSize.Label" ) );
    props.setLook( wlBatchSize );
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.top = new FormAttachment( wRetrieveStep, 10 );
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    wlBatchSize.setLayoutData( fdlBatchSize );

    wBatchSize = new TextVar( pipelineMeta, wOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( 0, 0 );
    fdBatchSize.top = new FormAttachment( wlBatchSize, 5 );
    wBatchSize.setLayoutData( fdBatchSize );

    Label wlBatchTime = new Label( wOptions, SWT.LEFT );
    wlBatchTime.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.BatchTime.Label" ) );
    props.setLook( wlBatchTime );
    FormData fdlBatchTime = new FormData();
    fdlBatchTime.top = new FormAttachment( wBatchSize, 10 );
    fdlBatchTime.left = new FormAttachment( 0, 0 );
    wlBatchTime.setLayoutData( fdlBatchTime );

    wBatchTime = new TextVar( pipelineMeta, wOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wBatchTime.addModifyListener( lsMod );
    FormData fdBatchTime = new FormData();
    fdBatchTime.left = new FormAttachment( 0, 0 );
    fdBatchTime.top = new FormAttachment( wlBatchTime, 5 );
    wBatchTime.setLayoutData( fdBatchTime );

    wOptionsTab.setControl( wOptions );

    FormData fdOptions = new FormData();
    fdOptions.left = new FormAttachment( 0, 0 );
    fdOptions.top = new FormAttachment( 0, 0 );
    fdOptions.right = new FormAttachment( 100, 0 );
    fdOptions.bottom = new FormAttachment( 100, 0 );
    wOptions.setLayoutData( fdOptions );
    // Options Tab End

    // Parameters Tab Start

    CTabItem wParametersTab = new CTabItem( wTabFolder, SWT.NONE );
    wParametersTab.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.Fields.Parameters.Label" ) );

    FormLayout fieldLayout = new FormLayout();
    fieldLayout.marginWidth = 15;
    fieldLayout.marginHeight = 15;

    Composite wParameterComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wParameterComp );
    wParameterComp.setLayout( fieldLayout );

    // Pass all parameters down
    //
    wPassParams = new Button( wParameterComp, SWT.CHECK );
    props.setLook( wPassParams );
    wPassParams.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.PassAllParameters.Label" ) );
    FormData fdPassParams = new FormData();
    fdPassParams.left = new FormAttachment( 0, 0 );
    fdPassParams.top = new FormAttachment( 0, 0 );
    wPassParams.setLayoutData( fdPassParams );
    wPassParams.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        changed = true;
      }
    } );

    wbGetParams = new Button( wParameterComp, SWT.PUSH );
    wbGetParams.setText( BaseMessages.getString( PKG, "SingleThreaderDialog.GetParameters.Button.Label" ) );
    FormData fdGetParams = new FormData();
    fdGetParams.bottom = new FormAttachment( 100, 0 );
    fdGetParams.right = new FormAttachment( 100, 0 );
    wbGetParams.setLayoutData( fdGetParams );
    wbGetParams.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        getParameters( null ); // null: force reload of file from specification
      }
    } );

    final int parameterRows =
      singleThreaderMeta.getParameters() != null ? singleThreaderMeta.getParameters().length : 0;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "SingleThreaderDialog.Parameters.Parameter.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SingleThreaderDialog.Parameters.Value.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };
    colinf[ 1 ].setUsingVariables( true );

    wParameters =
      new TableView( pipelineMeta, wParameterComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, parameterRows,
        false, lsMod, props, false );

    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment( 0, 0 );
    fdParameters.top = new FormAttachment( wPassParams, 10 );
    fdParameters.right = new FormAttachment( 100 );
    fdParameters.bottom = new FormAttachment( wbGetParams, -10 );
    wParameters.setLayoutData( fdParameters );
    wParameters.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 50, 50 ) );

    FormData fdParametersComp = new FormData();
    fdParametersComp.left = new FormAttachment( 0, 0 );
    fdParametersComp.top = new FormAttachment( 0, 0 );
    fdParametersComp.right = new FormAttachment( 100, 0 );
    fdParametersComp.bottom = new FormAttachment( 100, 0 );
    wParameterComp.setLayoutData( fdParametersComp );

    wParameterComp.layout();
    wParametersTab.setControl( wParameterComp );

    wTabFolder.setSelection( 0 );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100, 0 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );

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

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wPath.addSelectionListener( lsDef );

    wStepname.addSelectionListener( lsDef );
    wBatchSize.addSelectionListener( lsDef );
    wBatchTime.addSelectionListener( lsDef );
    wInjectStep.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    singleThreaderMeta.setChanged( changed );

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
      .getImage( shell.getDisplay(), getClass().getClassLoader(), "MAP.svg", ConstUI.LARGE_ICON_SIZE,
        ConstUI.LARGE_ICON_SIZE );
  }

  private void selectPipelineFile( boolean useVfs ) {
    String curFile = pipelineMeta.environmentSubstitute( wPath.getText() );

    if ( useVfs ) {
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
          BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingPipeline.DialogTitle" ),
          BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingPipeline.DialogMessage" ), e );
      }
    }
  }

  private void loadPipelineFile( String fname ) throws HopException {
    mappingPipelineMeta = new PipelineMeta( pipelineMeta.environmentSubstitute( fname ) );
    mappingPipelineMeta.clearChanged();
  }

  private void loadPipeline() throws HopException {
    String filename = wPath.getText();
    if ( Utils.isEmpty( filename ) ) {
      return;
    }
    if ( !filename.endsWith( ".hpl" ) ) {
      filename = filename + ".hpl";
      wPath.setText( filename );
    }
    loadPipelineFile( filename );

    wInjectStep.setText( getInjectorStep( mappingPipelineMeta ) );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wPath.setText( Const.NVL( singleThreaderMeta.getFileName(), "" ) );

    wBatchSize.setText( Const.NVL( singleThreaderMeta.getBatchSize(), "" ) );
    wBatchTime.setText( Const.NVL( singleThreaderMeta.getBatchTime(), "" ) );
    wInjectStep.setText( Const.NVL( singleThreaderMeta.getInjectStep(), "" ) );
    wRetrieveStep.setText( Const.NVL( singleThreaderMeta.getRetrieveStep(), "" ) );

    // Parameters
    //
    if ( singleThreaderMeta.getParameters() != null ) {
      for ( int i = 0; i < singleThreaderMeta.getParameters().length; i++ ) {
        TableItem ti = wParameters.table.getItem( i );
        if ( !Utils.isEmpty( singleThreaderMeta.getParameters()[ i ] ) ) {
          ti.setText( 1, Const.NVL( singleThreaderMeta.getParameters()[ i ], "" ) );
          ti.setText( 2, Const.NVL( singleThreaderMeta.getParameterValues()[ i ], "" ) );
        }
      }
      wParameters.removeEmptyRows();
      wParameters.setRowNums();
      wParameters.optWidth( true );
    }

    wPassParams.setSelection( singleThreaderMeta.isPassingAllParameters() );

    try {
      loadPipeline();
    } catch ( Throwable t ) {
      // Skip the error, it becomes annoying otherwise
    }

    wStepname.selectAll();
    wStepname.setFocus();
  }

  public static String getInjectorStep( PipelineMeta mappingPipelineMeta ) {
    for ( StepMeta stepMeta : mappingPipelineMeta.getSteps() ) {
      if ( stepMeta.getStepID().equals( "Injector" ) || stepMeta.getStepID().equals( "MappingInput" ) ) {
        return stepMeta.getName();
      }
    }
    return "";
  }

  private void cancel() {
    stepname = null;
    singleThreaderMeta.setChanged( changed );
    dispose();
  }

  private void getInfo( SingleThreaderMeta meta ) throws HopException {
    loadPipeline();
    meta.setFileName( wPath.getText() );
    meta.setBatchSize( wBatchSize.getText() );
    meta.setBatchTime( wBatchTime.getText() );
    meta.setInjectStep( wInjectStep.getText() );
    meta.setRetrieveStep( wRetrieveStep.getText() );

    // The parameters...
    //
    int nritems = wParameters.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      if ( !Utils.isEmpty( param ) ) {
        nr++;
      }
    }
    meta.setParameters( new String[ nr ] );
    meta.setParameterValues( new String[ nr ] );
    nr = 0;
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nritems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      String value = wParameters.getNonEmpty( i ).getText( 2 );

      meta.getParameters()[ nr ] = param;
      meta.getParameterValues()[ nr ] = Const.NVL( value, "" );

      nr++;
    }

    meta.setPassingAllParameters( wPassParams.getSelection() );
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    try {
      getInfo( singleThreaderMeta );
      loadPipeline();
    } catch ( HopException e ) {
      new ErrorDialog( shell, BaseMessages.getString(
        PKG, "SingleThreaderDialog.ErrorLoadingSpecifiedPipeline.Title" ), BaseMessages.getString(
        PKG, "SingleThreaderDialog.ErrorLoadingSpecifiedPipeline.Message" ), e );
    }

    dispose();
  }

  protected void getParameters( PipelineMeta mappingPipelineMeta ) {
    try {
      if ( mappingPipelineMeta == null ) {
        SingleThreaderMeta jet = new SingleThreaderMeta();
        getInfo( jet );
        mappingPipelineMeta = SingleThreaderMeta.loadSingleThreadedPipelineMeta( jet, pipelineMeta );
      }
      String[] parameters = mappingPipelineMeta.listParameters();

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
      new ErrorDialog( shell, BaseMessages.getString(
        PKG, "SingleThreaderDialog.Exception.UnableToLoadPipeline.Title" ), BaseMessages.getString(
        PKG, "SingleThreaderDialog.Exception.UnableToLoadPipeline.Message" ), e );
    }
  }
}
