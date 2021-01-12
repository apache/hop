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

package org.apache.hop.pipeline.transforms.flattener;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class FlattenerDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = FlattenerMeta.class; // For Translator

  private TableView wFields;

  private CCombo wField;

  private boolean gotPreviousFields = false;

  private FlattenerMeta input;

  public FlattenerDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (FlattenerMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "FlattenerDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection,  e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );


    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "FlattenerDialog.TransformName.Label" ) );
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

    // Key field...
    Label wlField = new Label(shell, SWT.RIGHT);
    wlField.setText( BaseMessages.getString( PKG, "FlattenerDialog.FlattenField.Label" ) );
    props.setLook(wlField);
    FormData fdlField = new FormData();
    fdlField.left = new FormAttachment( 0, 0 );
    fdlField.right = new FormAttachment( middle, -margin );
    fdlField.top = new FormAttachment( wTransformName, margin );
    wlField.setLayoutData(fdlField);
    wField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wField );
    wField.addModifyListener( lsMod );
    FormData fdField = new FormData();
    fdField.left = new FormAttachment( middle, 0 );
    fdField.top = new FormAttachment( wTransformName, margin );
    fdField.right = new FormAttachment( 100, 0 );
    wField.setLayoutData(fdField);
    wField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "FlattenerDialog.TargetField.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wField, margin );
    wlFields.setLayoutData(fdlFields);

    int nrKeyCols = 1;
    int nrKeyRows = ( input.getTargetField() != null ? input.getTargetField().length : 1 );

    ColumnInfo[] ciKey = new ColumnInfo[ nrKeyCols ];
    ciKey[ 0 ] = new ColumnInfo( BaseMessages.getString( PKG, "FlattenerDialog.ColumnInfo.TargetField" ), ColumnInfo.COLUMN_TYPE_TEXT, false );

    wFields = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey, nrKeyRows, lsMod, props );
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, -margin );
    fdFields.bottom = new FormAttachment( wOk, -2*margin );
    wFields.setLayoutData(fdFields);

    // Add listeners
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( backupChanged );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void getFields() {
    if ( !gotPreviousFields ) {
      try {
        String field = wField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wField.setItems( r.getFieldNames() );
        }
        if ( field != null ) {
          wField.setText( field );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "FlattenerDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "FlattenerDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "FlattenerDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getFieldName() != null ) {
      wField.setText( input.getFieldName() );
    }

    if ( input.getTargetField() != null ) {
      for ( int i = 0; i < input.getTargetField().length; i++ ) {
        TableItem item = wFields.table.getItem( i );
        if ( input.getTargetField()[ i ] != null ) {
          item.setText( 1, input.getTargetField()[ i ] );
        }
      }
    }

    wFields.setRowNums();
    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    int nrTargets = wFields.nrNonEmpty();

    input.setFieldName( wField.getText() );

    input.allocate( nrTargets );

    for ( int i = 0; i < nrTargets; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      //CHECKSTYLE:Indentation:OFF
      input.getTargetField()[ i ] = item.getText( 1 );
    }
    transformName = wTransformName.getText();

    dispose();
  }
}
