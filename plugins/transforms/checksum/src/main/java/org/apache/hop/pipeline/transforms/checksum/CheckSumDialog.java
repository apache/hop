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

package org.apache.hop.pipeline.transforms.checksum;



import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class CheckSumDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = CheckSumDialog.class; // For Translator

  private final CheckSumMeta input;

  private CCombo wType;

  private TableView wFields;

  private Text wResult;

  private ColumnInfo[] colinf;

  private final Map<String, Integer> inputFields;

  private Label wlResultType;
  private CCombo wResultType;

  public CheckSumDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (CheckSumMeta) in;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CheckSumDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "CheckSumDialog.TransformName.Label" ) );
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

    // Type
    Label wlType = new Label(shell, SWT.RIGHT);
    wlType.setText( BaseMessages.getString( PKG, "CheckSumDialog.Type.Label" ) );
    props.setLook(wlType);
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment( 0, 0 );
    fdlType.right = new FormAttachment( middle, -margin );
    fdlType.top = new FormAttachment( wTransformName, margin );
    wlType.setLayoutData(fdlType);
    wType = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    for ( int i = 0; i < input.getChecksumtypeDescs().length; i++ ) {
      wType.add( input.getChecksumtypeDescs()[ i ] );
    }
    wType.select( 0 );
    props.setLook( wType );
    FormData fdType = new FormData();
    fdType.left = new FormAttachment( middle, 0 );
    fdType.top = new FormAttachment( wTransformName, margin );
    fdType.right = new FormAttachment( 100, 0 );
    wType.setLayoutData(fdType);
    wType.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        activeResultType();
      }
    } );

    // ResultType
    wlResultType = new Label( shell, SWT.RIGHT );
    wlResultType.setText( BaseMessages.getString( PKG, "CheckSumDialog.ResultType.Label" ) );
    props.setLook( wlResultType );
    FormData fdlResultType = new FormData();
    fdlResultType.left = new FormAttachment( 0, 0 );
    fdlResultType.right = new FormAttachment( middle, -margin );
    fdlResultType.top = new FormAttachment( wType, 2 * margin );
    wlResultType.setLayoutData(fdlResultType);
    wResultType = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wResultType.setItems( input.getResultTypeDescs() );
    wResultType.select( 0 );
    props.setLook( wResultType );
    FormData fdResultType = new FormData();
    fdResultType.left = new FormAttachment( middle, 0 );
    fdResultType.top = new FormAttachment( wType, 2 * margin );
    fdResultType.right = new FormAttachment( 100, 0 );
    wResultType.setLayoutData(fdResultType);
    wResultType.addSelectionListener( new SelectionAdapter() {

      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Result line...
    Label wlResult = new Label(shell, SWT.RIGHT);
    wlResult.setText( BaseMessages.getString( PKG, "CheckSumDialog.Result.Label" ) );
    props.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wResultType, margin * 2 );
    wlResult.setLayoutData(fdlResult);
    wResult = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wResultType, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData(fdResult);

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );

    // Table with fields
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "CheckSumDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wResult, margin );
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 1;
    final int FieldsRows = input.getFieldName().length;

    colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "CheckSumDialog.Fieldname.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2 * margin );
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );

          // Remember these fields...
          for ( int i = 0; i < row.size(); i++ ) {
            inputFields.put( row.getValueMeta( i ).getName(), i);
          }

          setComboBoxes();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    new Thread( runnable ).start();

    // Add listeners
    lsCancel = e -> cancel();
    lsGet = e -> get();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );
    wGet.addListener( SWT.Selection, lsGet );

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

    getData();    
    activeResultType();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void activeResultType() {
    int currentType = wType.getSelectionIndex();
    // Only available for type MD5 and SHA
    boolean active = currentType == 2 || currentType == 3 || currentType == 4 || currentType == 5 || currentType == 6;
    wlResultType.setEnabled( active );
    wResultType.setEnabled( active );
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    colinf[ 0 ].setComboValues( fieldNames );
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener insertListener = ( tableItem, v ) -> {
          tableItem.setText( 2, BaseMessages.getString( PKG, "System.Combo.Yes" ) );
          return true;
        };
        BaseTransformDialog
          .getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, insertListener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wType.select( input.getTypeByDesc() );
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }
    wResultType.setText( input.getResultTypeDesc( input.getResultType() ) );
   
    Table table = wFields.table;
    if ( input.getFieldName().length > 0 ) {
      table.removeAll();
    }
    for ( int i = 0; i < input.getFieldName().length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      ti.setText( 1, input.getFieldName()[ i ] );
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

    if ( wType.getSelectionIndex() < 0 ) {
      input.setCheckSumType( 0 );
    } else {
      input.setCheckSumType( wType.getSelectionIndex() );
    }

    input.setResultFieldName( wResult.getText() );
    input.setResultType( input.getResultTypeByDesc( wResultType.getText() ) );

    int nrFields = wFields.nrNonEmpty();
    input.allocate( nrFields );
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem ti = wFields.getNonEmpty( i );
      //CHECKSTYLE:Indentation:OFF
      input.getFieldName()[ i ] = ti.getText( 1 );
    }
    dispose();
  }
}
