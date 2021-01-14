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

package org.apache.hop.pipeline.transforms.valuemapper;

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

public class ValueMapperDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ValueMapperMeta.class; // For Translator

  private Text wTransformName;

  private CCombo wFieldName;

  private Text wTargetFieldName;

  private Text wNonMatchDefault;

  private TableView wFields;

  private final ValueMapperMeta input;

  private boolean gotPreviousFields = false;

  public ValueMapperDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ValueMapperMeta) in;
  }

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
    shell.setText( BaseMessages.getString( PKG, "ValueMapperDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // TransformName line
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText( BaseMessages.getString( PKG, "ValueMapperDialog.TransformName.Label" ) );
    props.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData(fdTransformName);

    // Fieldname line
    Label wlFieldname = new Label(shell, SWT.RIGHT);
    wlFieldname.setText( BaseMessages.getString( PKG, "ValueMapperDialog.FieldnameToUser.Label" ) );
    props.setLook(wlFieldname);
    FormData fdlFieldname = new FormData();
    fdlFieldname.left = new FormAttachment( 0, 0 );
    fdlFieldname.right = new FormAttachment( middle, -margin );
    fdlFieldname.top = new FormAttachment( wTransformName, margin );
    wlFieldname.setLayoutData(fdlFieldname);

    wFieldName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wFieldName );
    wFieldName.addModifyListener( lsMod );
    FormData fdFieldname = new FormData();
    fdFieldname.left = new FormAttachment( middle, 0 );
    fdFieldname.top = new FormAttachment( wTransformName, margin );
    fdFieldname.right = new FormAttachment( 100, 0 );
    wFieldName.setLayoutData(fdFieldname);
    wFieldName.addFocusListener( new FocusListener() {
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
    // TargetFieldname line
    Label wlTargetFieldname = new Label(shell, SWT.RIGHT);
    wlTargetFieldname.setText( BaseMessages.getString( PKG, "ValueMapperDialog.TargetFieldname.Label" ) );
    props.setLook(wlTargetFieldname);
    FormData fdlTargetFieldname = new FormData();
    fdlTargetFieldname.left = new FormAttachment( 0, 0 );
    fdlTargetFieldname.right = new FormAttachment( middle, -margin );
    fdlTargetFieldname.top = new FormAttachment( wFieldName, margin );
    wlTargetFieldname.setLayoutData(fdlTargetFieldname);
    wTargetFieldName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetFieldName );
    wTargetFieldName.addModifyListener( lsMod );
    FormData fdTargetFieldname = new FormData();
    fdTargetFieldname.left = new FormAttachment( middle, 0 );
    fdTargetFieldname.top = new FormAttachment( wFieldName, margin );
    fdTargetFieldname.right = new FormAttachment( 100, 0 );
    wTargetFieldName.setLayoutData(fdTargetFieldname);

    // Non match default line
    Label wlNonMatchDefault = new Label(shell, SWT.RIGHT);
    wlNonMatchDefault.setText( BaseMessages.getString( PKG, "ValueMapperDialog.NonMatchDefault.Label" ) );
    props.setLook(wlNonMatchDefault);
    FormData fdlNonMatchDefault = new FormData();
    fdlNonMatchDefault.left = new FormAttachment( 0, 0 );
    fdlNonMatchDefault.right = new FormAttachment( middle, -margin );
    fdlNonMatchDefault.top = new FormAttachment( wTargetFieldName, margin );
    wlNonMatchDefault.setLayoutData(fdlNonMatchDefault);
    wNonMatchDefault = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNonMatchDefault );
    wNonMatchDefault.addModifyListener( lsMod );
    FormData fdNonMatchDefault = new FormData();
    fdNonMatchDefault.left = new FormAttachment( middle, 0 );
    fdNonMatchDefault.top = new FormAttachment( wTargetFieldName, margin );
    fdNonMatchDefault.right = new FormAttachment( 100, 0 );
    wNonMatchDefault.setLayoutData(fdNonMatchDefault);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "ValueMapperDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wNonMatchDefault, margin );
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 2;
    final int FieldsRows = input.getSourceValue().length;

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ValueMapperDialog.Fields.Column.SourceValue" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ValueMapperDialog.Fields.Column.TargetValue" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
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
    input.setChanged( changed );

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
      gotPreviousFields = true;
      try {
        String fieldname = wFieldName.getText();

        wFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wFieldName.setItems( r.getFieldNames() );
          if ( fieldname != null ) {
            wFieldName.setText( fieldname );
          }
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "ValueMapperDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "ValueMapperDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wTransformName.setText( transformName );

    if ( input.getFieldToUse() != null ) {
      wFieldName.setText( input.getFieldToUse() );
    }
    if ( input.getTargetField() != null ) {
      wTargetFieldName.setText( input.getTargetField() );
    }
    if ( input.getNonMatchDefault() != null ) {
      wNonMatchDefault.setText( input.getNonMatchDefault() );
    }

    for ( int i = 0; i < input.getSourceValue().length; i++ ) {
      TableItem item = wFields.table.getItem( i );
      String src = input.getSourceValue()[ i ];
      String tgt = input.getTargetValue()[ i ];

      if ( src != null ) {
        item.setText( 1, src );
      }
      if ( tgt != null ) {
        item.setText( 2, tgt );
      }
    }

    wFields.setRowNums();
    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    transformName = wTransformName.getText(); // return value

    input.setFieldToUse( wFieldName.getText() );
    input.setTargetField( wTargetFieldName.getText() );
    input.setNonMatchDefault( wNonMatchDefault.getText() );

    int count = wFields.nrNonEmpty();
    input.allocate( count );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < count; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getSourceValue()[ i ] = Utils.isEmpty( item.getText( 1 ) ) ? null : item.getText( 1 );
      input.getTargetValue()[ i ] = item.getText( 2 );
    }
    dispose();
  }
}
