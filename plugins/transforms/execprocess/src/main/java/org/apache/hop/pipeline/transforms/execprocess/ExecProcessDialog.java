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

package org.apache.hop.pipeline.transforms.execprocess;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ExecProcessDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ExecProcessMeta.class; // For Translator

  private CCombo wProcess;

  private Button wArgumentsInFields;

  private TableView wArgumentFields;

  private Button wFailWhenNotSuccess;

  private LabelTextVar wOutputDelim, wResult, wExitValue, wError;

  private final ExecProcessMeta input;
  private boolean gotPreviousFields = false;

  public ExecProcessDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ExecProcessMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ExecProcessDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ExecProcessDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // The Tab Folders
    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ///////////////////////
    // START OF GENERAL TAB //
    // ///////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "ExecProcessDialog.GeneralTab.TabItem" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = margin;
    generalLayout.marginHeight = margin;
    wGeneralComp.setLayout( generalLayout );

    // filename field
    Label wlProcess = new Label(wGeneralComp, SWT.RIGHT);
    wlProcess.setText( BaseMessages.getString( PKG, "ExecProcessDialog.Process.Label" ) );
    props.setLook(wlProcess);
    FormData fdlProcess = new FormData();
    fdlProcess.left = new FormAttachment( 0, 0 );
    fdlProcess.right = new FormAttachment( middle, -margin );
    fdlProcess.top = new FormAttachment( wTransformName, margin );
    wlProcess.setLayoutData(fdlProcess);

    wProcess = new CCombo(wGeneralComp, SWT.BORDER | SWT.READ_ONLY );
    wProcess.setEditable( true );
    props.setLook( wProcess );
    wProcess.addModifyListener( lsMod );
    FormData fdProcess = new FormData();
    fdProcess.left = new FormAttachment( middle, 0 );
    fdProcess.top = new FormAttachment( wTransformName, margin );
    fdProcess.right = new FormAttachment( 100, -margin );
    wProcess.setLayoutData(fdProcess);
    wProcess.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( FocusEvent e ) {
      }

      @Override
      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Command Arguments are in separate fields
    Label wlArgumentsInFields = new Label(wGeneralComp, SWT.RIGHT);
    wlArgumentsInFields.setText( BaseMessages.getString( PKG, "ExecProcessDialog.ArgumentInFields.Label" ) );
    props.setLook(wlArgumentsInFields);
    FormData fdlArgumentsInFields = new FormData();
    fdlArgumentsInFields.left = new FormAttachment( 0, 0 );
    fdlArgumentsInFields.top = new FormAttachment( wProcess, margin );
    fdlArgumentsInFields.right = new FormAttachment( middle, -margin );
    wlArgumentsInFields.setLayoutData(fdlArgumentsInFields);
    wArgumentsInFields = new Button(wGeneralComp, SWT.CHECK );
    wArgumentsInFields.setToolTipText( BaseMessages.getString( PKG, "ExecProcessDialog.ArgumentInFields.Tooltip" ) );
    props.setLook( wArgumentsInFields );
    FormData fdArgumentsInFields = new FormData();
    fdArgumentsInFields.left = new FormAttachment( middle, 0 );
    fdArgumentsInFields.top = new FormAttachment( wlArgumentsInFields, 0, SWT.CENTER );
    fdArgumentsInFields.right = new FormAttachment( 100, 0 );
    wArgumentsInFields.setLayoutData(fdArgumentsInFields);
    wArgumentsInFields.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        enableFields();
        input.setChanged();
      }
    } );

    // Fail when status is different than 0
    Label wlFailWhenNotSuccess = new Label(wGeneralComp, SWT.RIGHT);
    wlFailWhenNotSuccess.setText( BaseMessages.getString( PKG, "ExecProcessDialog.FailWhenNotSuccess.Label" ) );
    props.setLook(wlFailWhenNotSuccess);
    FormData fdlFailWhenNotSuccess = new FormData();
    fdlFailWhenNotSuccess.left = new FormAttachment( 0, 0 );
    fdlFailWhenNotSuccess.top = new FormAttachment( wArgumentsInFields, margin );
    fdlFailWhenNotSuccess.right = new FormAttachment( middle, -margin );
    wlFailWhenNotSuccess.setLayoutData(fdlFailWhenNotSuccess);
    wFailWhenNotSuccess = new Button(wGeneralComp, SWT.CHECK );
    wFailWhenNotSuccess.setToolTipText( BaseMessages.getString(
      PKG, "ExecProcessDialog.FailWhenNotSuccess.Tooltip" ) );
    props.setLook( wFailWhenNotSuccess );
    FormData fdFailWhenNotSuccess = new FormData();
    fdFailWhenNotSuccess.left = new FormAttachment( middle, 0 );
    fdFailWhenNotSuccess.top = new FormAttachment( wlFailWhenNotSuccess, 0, SWT.CENTER );
    fdFailWhenNotSuccess.right = new FormAttachment( 100, 0 );
    wFailWhenNotSuccess.setLayoutData(fdFailWhenNotSuccess);
    wFailWhenNotSuccess.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // List of Argument Fields when ArgumentsInFields is enabled
    Label wlArgumentFields = new Label(wGeneralComp, SWT.LEFT);
    wlArgumentFields.setText( BaseMessages.getString( PKG, "ExecProcessDialog.ArgumentFields.Label" ) );
    props.setLook(wlArgumentFields);
    FormData fdlArgumentFields = new FormData();
    fdlArgumentFields.left = new FormAttachment( 0, 0 );
    fdlArgumentFields.top = new FormAttachment( wFailWhenNotSuccess, margin );
    fdlArgumentFields.right = new FormAttachment( middle, -margin );
    wlArgumentFields.setLayoutData(fdlArgumentFields);
    ColumnInfo[] colinf = new ColumnInfo[ 1 ];
    colinf[ 0 ] = new ColumnInfo(
      BaseMessages.getString( PKG, "ExecProcessDialog.ArgumentField.Label" ),
      ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "ExecProcessDialog.ArgumentField.Tooltip" ) );
    wArgumentFields =
      new TableView(
        null, wGeneralComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props );
    FormData fdArgumentFields = new FormData();
    fdArgumentFields.left = new FormAttachment( 0, 0 );
    fdArgumentFields.top = new FormAttachment(wlArgumentFields, margin );
    fdArgumentFields.right = new FormAttachment( 100, 0 );
    fdArgumentFields.bottom = new FormAttachment( 100, -margin );
    wArgumentFields.setLayoutData(fdArgumentFields);

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // /////////////////////
    // END OF GENERAL TAB //
    // /////////////////////

    // //////////////////////
    // START OF OUTPUT TAB //
    // //////////////////////

    CTabItem wOutputTab = new CTabItem(wTabFolder, SWT.NONE);
    wOutputTab.setText( BaseMessages.getString( PKG, "ExecProcessDialog.Output.TabItem" ) );

    Composite wOutputComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wOutputComp);

    FormLayout fdOutputCompLayout = new FormLayout();
    fdOutputCompLayout.marginWidth = margin;
    fdOutputCompLayout.marginHeight = margin;
    wOutputComp.setLayout( fdOutputCompLayout );

    // Output Line Delimiter
    wOutputDelim = new LabelTextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER,
      BaseMessages.getString( PKG, "ExecProcessDialog.OutputDelimiterField.Label" ),
      BaseMessages.getString( PKG, "ExecProcessDialog.OutputDelimiterField.Tooltip" ) );
    wOutputDelim.addModifyListener( lsMod );
    FormData fdOutputDelim = new FormData();
    fdOutputDelim.left = new FormAttachment( 0, 0 );
    fdOutputDelim.top = new FormAttachment( 0, margin );
    fdOutputDelim.right = new FormAttachment( 100, 0 );
    wOutputDelim.setLayoutData(fdOutputDelim);

    // Result fieldname ...
    wResult = new LabelTextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER,
      BaseMessages.getString( PKG, "ExecProcessDialog.ResultField.Label" ),
      BaseMessages.getString( PKG, "ExecProcessDialog.ResultField.Tooltip" ) );
    wResult.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( 0, 0 );
    fdResult.top = new FormAttachment( wOutputDelim, margin );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData(fdResult);

    // Error fieldname ...
    wError = new LabelTextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER,
      BaseMessages.getString( PKG, "ExecProcessDialog.ErrorField.Label" ),
      BaseMessages.getString( PKG, "ExecProcessDialog.ErrorField.Tooltip" ) );
    wError.addModifyListener( lsMod );
    FormData fdError = new FormData();
    fdError.left = new FormAttachment( 0, 0 );
    fdError.top = new FormAttachment( wResult, margin );
    fdError.right = new FormAttachment( 100, 0 );
    wError.setLayoutData(fdError);

    // ExitValue fieldname ...
    wExitValue = new LabelTextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER,
      BaseMessages.getString( PKG, "ExecProcessDialog.ExitValueField.Label" ),
      BaseMessages.getString( PKG, "ExecProcessDialog.ExitValueField.Tooltip" ) );
    wExitValue.addModifyListener( lsMod );
    FormData fdExitValue = new FormData();
    fdExitValue.left = new FormAttachment( 0, 0 );
    fdExitValue.top = new FormAttachment( wError, margin );
    fdExitValue.right = new FormAttachment( 100, 0 );
    wExitValue.setLayoutData(fdExitValue);

    FormData fdOutputComp = new FormData();
    fdOutputComp.left = new FormAttachment( 0, 0 );
    fdOutputComp.top = new FormAttachment( 0, 0 );
    fdOutputComp.right = new FormAttachment( 100, 0 );
    fdOutputComp.bottom = new FormAttachment( 100, 0 );
    wOutputComp.setLayoutData( fdOutputComp );

    wOutputComp.layout();
    wOutputTab.setControl(wOutputComp);

    // ////////////////////
    // END OF OUTPUT TAB //
    // ////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData( fdTabFolder );

    wTabFolder.setSelection( 0 );

    // ////////////////////
    // END OF TAB FOLDER //
    // ////////////////////



    // Add listeners
    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    IRowMeta r = null;
    try {
      r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        wArgumentFields.getColumns()[ 0 ].setComboValues( r.getFieldNames() );
      }
    } catch ( HopTransformException ignore ) {
      // Do nothing
    }


    getData();
    enableFields();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "ExecProcessDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getProcessField() != null ) {
      wProcess.setText( input.getProcessField() );
    }
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }
    if ( input.getErrorFieldName() != null ) {
      wError.setText( input.getErrorFieldName() );
    }
    if ( input.getExitValueFieldName() != null ) {
      wExitValue.setText( input.getExitValueFieldName() );
    }
    if ( input.getOutputLineDelimiter() != null ) {
      wOutputDelim.setText( input.getOutputLineDelimiter() );
    }
    wFailWhenNotSuccess.setSelection( input.isFailWhenNotSuccess() );
    wArgumentsInFields.setSelection( input.isArgumentsInFields() );
    int nrRows = input.getArgumentFieldNames().length;
    if ( nrRows <= 0 ) {
      wArgumentFields.getTable().setItemCount( 1 );
    } else {
      wArgumentFields.getTable().setItemCount( nrRows );
      for ( int i = 0; i < input.getArgumentFieldNames().length; i++ ) {
        TableItem item = wArgumentFields.getTable().getItem( i );
        item.setText( 1, input.getArgumentFieldNames()[ i ] );
      }
    }
    wArgumentFields.setRowNums();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void enableFields() {
    wArgumentFields.setEnabled( wArgumentsInFields.getSelection() );
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    input.setProcessField( wProcess.getText() );
    input.setResultFieldName( wResult.getText() );
    input.setErrorFieldName( wError.getText() );
    input.setExitValueFieldName( wExitValue.getText() );
    input.setFailWhenNotSuccess( wFailWhenNotSuccess.getSelection() );
    input.setOutputLineDelimiter( wOutputDelim.getText() );
    input.setArgumentsInFields( wArgumentsInFields.getSelection() );
    String[] argumentFields = null;
    if ( wArgumentsInFields.getSelection() ) {
      argumentFields = new String[ wArgumentFields.nrNonEmpty() ];
    } else {
      argumentFields = new String[ 0 ];
    }
    for ( int i = 0; i < argumentFields.length; i++ ) {
      argumentFields[ i ] = wArgumentFields.getNonEmpty( i ).getText( 1 );
    }
    input.setArgumentFieldNames( argumentFields );
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String fieldvalue = wProcess.getText();
        wProcess.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wProcess.setItems( r.getFieldNames() );
        }
        if ( fieldvalue != null ) {
          wProcess.setText( fieldvalue );
        }
        gotPreviousFields = true;
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "ExecProcessDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "ExecProcessDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }
}
