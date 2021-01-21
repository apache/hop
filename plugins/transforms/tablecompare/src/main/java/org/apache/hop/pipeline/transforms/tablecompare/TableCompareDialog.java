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

package org.apache.hop.pipeline.transforms.tablecompare;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.LabelCombo;
import org.apache.hop.ui.core.widget.LabelText;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.Arrays;
import org.apache.hop.core.variables.IVariables;

public class TableCompareDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = TableCompare.class; // For Translator

  private final TableCompareMeta input;

  /**
   * all fields from the previous transforms
   */
  private IRowMeta prevFields = null;

  private LabelCombo wReferenceDB;
  private LabelCombo wReferenceSchema;
  private LabelCombo wReferenceTable;

  private LabelCombo wCompareDB;
  private LabelCombo wCompareSchema;
  private LabelCombo wCompareTable;

  private LabelCombo wKeyFields;
  private LabelCombo wExcludeFields;
  private LabelText wNrErrors;

  private LabelText wNrRecordsReference;
  private LabelText wNrRecordsCompare;
  private LabelText wNrErrorsLeftJoin;
  private LabelText wNrErrorsInnerJoin;
  private LabelText wNrErrorsRightJoin;

  private LabelCombo wKeyDesc;
  private LabelCombo wReferenceValue;
  private LabelCombo wCompareValue;

  public TableCompareDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (TableCompareMeta) in;
  }

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

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "TableCompareDialog.Shell.Title" ) );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "TableCompareDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment( 0, margin);
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin);
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

    // Reference DB + schema + table
    //
    wReferenceDB = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceDB.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceDB.Tooltip" ) );
    props.setLook( wReferenceDB );
    FormData fdReferenceDB = new FormData();
    fdReferenceDB.left = new FormAttachment( 0, 0 );
    fdReferenceDB.top = new FormAttachment( lastControl, margin);
    fdReferenceDB.right = new FormAttachment( 100, 0 );
    wReferenceDB.setLayoutData( fdReferenceDB );
    lastControl = wReferenceDB;

    wReferenceSchema = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceSchemaField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceSchemaField.Tooltip" ) );
    props.setLook( wReferenceSchema );
    FormData fdReferenceSchema = new FormData();
    fdReferenceSchema.left = new FormAttachment( 0, 0 );
    fdReferenceSchema.top = new FormAttachment( lastControl, margin);
    fdReferenceSchema.right = new FormAttachment( 100, 0 );
    wReferenceSchema.setLayoutData( fdReferenceSchema );
    lastControl = wReferenceSchema;

    wReferenceTable = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceTableField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceTableField.Tooltip" ) );
    props.setLook( wReferenceTable );
    FormData fdReferenceTable = new FormData();
    fdReferenceTable.left = new FormAttachment( 0, 0 );
    fdReferenceTable.top = new FormAttachment( lastControl, margin);
    fdReferenceTable.right = new FormAttachment( 100, 0 );
    wReferenceTable.setLayoutData( fdReferenceTable );
    lastControl = wReferenceTable;

    // Reference DB + schema + table
    //
    wCompareDB = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.CompareDB.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.CompareDB.Tooltip" ) );
    props.setLook( wCompareDB );
    FormData fdCompareDB = new FormData();
    fdCompareDB.left = new FormAttachment( 0, 0 );
    fdCompareDB.top = new FormAttachment( lastControl, margin);
    fdCompareDB.right = new FormAttachment( 100, 0 );
    wCompareDB.setLayoutData( fdCompareDB );
    lastControl = wCompareDB;

    wCompareSchema = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.CompareSchemaField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.CompareSchemaField.Tooltip" ) );
    props.setLook( wCompareSchema );
    FormData fdCompareSchema = new FormData();
    fdCompareSchema.left = new FormAttachment( 0, 0 );
    fdCompareSchema.top = new FormAttachment( lastControl, margin);
    fdCompareSchema.right = new FormAttachment( 100, 0 );
    wCompareSchema.setLayoutData( fdCompareSchema );
    lastControl = wCompareSchema;

    wCompareTable = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.CompareTableField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.CompareTableField.Tooltip" ) );
    props.setLook( wCompareTable );
    FormData fdCompareTable = new FormData();
    fdCompareTable.left = new FormAttachment( 0, 0 );
    fdCompareTable.top = new FormAttachment( lastControl, margin);
    fdCompareTable.right = new FormAttachment( 100, 0 );
    wCompareTable.setLayoutData( fdCompareTable );
    lastControl = wCompareTable;

    wKeyFields = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.KeyFieldsField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.KeyFieldsField.Tooltip" ) );
    props.setLook( wKeyFields );
    FormData fdKeyFields = new FormData();
    fdKeyFields.left = new FormAttachment( 0, 0 );
    fdKeyFields.top = new FormAttachment( lastControl, margin);
    fdKeyFields.right = new FormAttachment( 100, 0 );
    wKeyFields.setLayoutData( fdKeyFields );
    lastControl = wKeyFields;

    wExcludeFields = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.ExcludeFieldsField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.ExcludeFieldsField.Tooltip" ) );
    props.setLook( wExcludeFields );
    FormData fdExcludeFields = new FormData();
    fdExcludeFields.left = new FormAttachment( 0, 0 );
    fdExcludeFields.top = new FormAttachment( lastControl, margin);
    fdExcludeFields.right = new FormAttachment( 100, 0 );
    wExcludeFields.setLayoutData( fdExcludeFields );
    lastControl = wExcludeFields;

    // The nr of errors field
    //
    wNrErrors = new LabelText( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsField.Tooltip" ) );
    props.setLook( wNrErrors );
    FormData fdNrErrors = new FormData();
    fdNrErrors.left = new FormAttachment( 0, 0 );
    fdNrErrors.top = new FormAttachment( lastControl, margin * 3 );
    fdNrErrors.right = new FormAttachment( 100, 0 );
    wNrErrors.setLayoutData( fdNrErrors );
    lastControl = wNrErrors;

    // The nr of records in the reference table
    //
    wNrRecordsReference = new LabelText( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.NrRecordsReferenceField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.NrRecordsReferenceField.Tooltip" ) );
    props.setLook( wNrRecordsReference );
    FormData fdNrRecordsReference = new FormData();
    fdNrRecordsReference.left = new FormAttachment( 0, 0 );
    fdNrRecordsReference.top = new FormAttachment( lastControl, margin);
    fdNrRecordsReference.right = new FormAttachment( 100, 0 );
    wNrRecordsReference.setLayoutData( fdNrRecordsReference );
    lastControl = wNrRecordsReference;

    // The nr of records in the Compare table
    //
    wNrRecordsCompare = new LabelText( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.NrRecordsCompareField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.NrRecordsCompareField.Tooltip" ) );
    props.setLook( wNrRecordsCompare );
    FormData fdNrRecordsCompare = new FormData();
    fdNrRecordsCompare.left = new FormAttachment( 0, 0 );
    fdNrRecordsCompare.top = new FormAttachment( lastControl, margin);
    fdNrRecordsCompare.right = new FormAttachment( 100, 0 );
    wNrRecordsCompare.setLayoutData( fdNrRecordsCompare );
    lastControl = wNrRecordsCompare;

    // The nr of errors in the left join
    //
    wNrErrorsLeftJoin = new LabelText( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsLeftJoinField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsLeftJoinField.Tooltip" ) );
    props.setLook( wNrErrorsLeftJoin );
    FormData fdNrErrorsLeftJoin = new FormData();
    fdNrErrorsLeftJoin.left = new FormAttachment( 0, 0 );
    fdNrErrorsLeftJoin.top = new FormAttachment( lastControl, margin);
    fdNrErrorsLeftJoin.right = new FormAttachment( 100, 0 );
    wNrErrorsLeftJoin.setLayoutData( fdNrErrorsLeftJoin );
    lastControl = wNrErrorsLeftJoin;

    // The nr of errors in the Inner join
    //
    wNrErrorsInnerJoin = new LabelText( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsInnerJoinField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsInnerJoinField.Tooltip" ) );
    props.setLook( wNrErrorsInnerJoin );
    FormData fdNrErrorsInnerJoin = new FormData();
    fdNrErrorsInnerJoin.left = new FormAttachment( 0, 0 );
    fdNrErrorsInnerJoin.top = new FormAttachment( lastControl, margin);
    fdNrErrorsInnerJoin.right = new FormAttachment( 100, 0 );
    wNrErrorsInnerJoin.setLayoutData( fdNrErrorsInnerJoin );
    lastControl = wNrErrorsInnerJoin;

    // The nr of errors in the Right join
    //
    wNrErrorsRightJoin = new LabelText( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsRightJoinField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.NrErrorsRightJoinField.Tooltip" ) );
    props.setLook( wNrErrorsRightJoin );
    FormData fdNrErrorsRightJoin = new FormData();
    fdNrErrorsRightJoin.left = new FormAttachment( 0, 0 );
    fdNrErrorsRightJoin.top = new FormAttachment( lastControl, margin);
    fdNrErrorsRightJoin.right = new FormAttachment( 100, 0 );
    wNrErrorsRightJoin.setLayoutData( fdNrErrorsRightJoin );
    lastControl = wNrErrorsRightJoin;

    wKeyDesc = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.KeyDescField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.KeyDescField.Tooltip" ) );
    props.setLook( wKeyDesc );
    FormData fdKeyDesc = new FormData();
    fdKeyDesc.left = new FormAttachment( 0, 0 );
    fdKeyDesc.top = new FormAttachment( lastControl, margin * 3 );
    fdKeyDesc.right = new FormAttachment( 100, 0 );
    wKeyDesc.setLayoutData( fdKeyDesc );
    lastControl = wKeyDesc;

    wReferenceValue = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceValueField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.ReferenceValueField.Tooltip" ) );
    props.setLook( wReferenceValue );
    FormData fdReferenceValue = new FormData();
    fdReferenceValue.left = new FormAttachment( 0, 0 );
    fdReferenceValue.top = new FormAttachment( lastControl, margin);
    fdReferenceValue.right = new FormAttachment( 100, 0 );
    wReferenceValue.setLayoutData( fdReferenceValue );
    lastControl = wReferenceValue;

    wCompareValue = new LabelCombo( shell,
      BaseMessages.getString( PKG, "TableCompareDialog.CompareValueField.Label" ),
      BaseMessages.getString( PKG, "TableCompareDialog.CompareValueField.Tooltip" ) );
    props.setLook( wCompareValue );
    FormData fdCompareValue = new FormData();
    fdCompareValue.left = new FormAttachment( 0, 0 );
    fdCompareValue.top = new FormAttachment( lastControl, margin);
    fdCompareValue.right = new FormAttachment( 100, 0 );
    wCompareValue.setLayoutData( fdCompareValue );
    lastControl = wCompareValue;

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, lastControl );

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    wNrErrors.addSelectionListener( lsDef );
    wNrRecordsReference.addSelectionListener( lsDef );
    wNrRecordsCompare.addSelectionListener( lsDef );
    wNrErrorsLeftJoin.addSelectionListener( lsDef );
    wNrErrorsInnerJoin.addSelectionListener( lsDef );
    wNrErrorsRightJoin.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    // Set the shell size, based upon previous time...
    //
    setSize();

    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setComboValues() {
    Runnable fieldLoader = new Runnable() {
      public void run() {

        try {
          prevFields = pipelineMeta.getPrevTransformFields( variables, transformName );

        } catch ( HopException e ) {
          String msg = BaseMessages.getString( PKG, "TableCompareDialog.DoMapping.UnableToFindInput" );
          log.logError( toString(), msg );
        }
        String[] prevTransformFieldNames = prevFields.getFieldNames();
        if ( prevTransformFieldNames != null ) {
          Arrays.sort( prevTransformFieldNames );

          wReferenceSchema.setItems( prevTransformFieldNames );
          wReferenceTable.setItems( prevTransformFieldNames );
          wCompareSchema.setItems( prevTransformFieldNames );
          wCompareTable.setItems( prevTransformFieldNames );
          wKeyFields.setItems( prevTransformFieldNames );
          wExcludeFields.setItems( prevTransformFieldNames );
          wKeyDesc.setItems( prevTransformFieldNames );
          wReferenceValue.setItems( prevTransformFieldNames );
          wCompareValue.setItems( prevTransformFieldNames );
        }
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    for ( DatabaseMeta dbMeta : pipelineMeta.getDatabases() ) {
      wReferenceDB.add( dbMeta.getName() );
      wCompareDB.add( dbMeta.getName() );
    }

    wReferenceDB.setText( input.getReferenceConnection() != null ? input.getReferenceConnection().getName() : "" );
    wReferenceSchema.setText( Const.NVL( input.getReferenceSchemaField(), "" ) );
    wReferenceTable.setText( Const.NVL( input.getReferenceTableField(), "" ) );
    wCompareDB.setText( input.getCompareConnection() != null ? input.getCompareConnection().getName() : "" );
    wCompareSchema.setText( Const.NVL( input.getCompareSchemaField(), "" ) );
    wCompareTable.setText( Const.NVL( input.getCompareTableField(), "" ) );
    wKeyFields.setText( Const.NVL( input.getKeyFieldsField(), "" ) );
    wExcludeFields.setText( Const.NVL( input.getExcludeFieldsField(), "" ) );

    wNrErrors.setText( Const.NVL( input.getNrErrorsField(), "" ) );
    wNrRecordsReference.setText( Const.NVL( input.getNrRecordsReferenceField(), "" ) );
    wNrRecordsCompare.setText( Const.NVL( input.getNrRecordsCompareField(), "" ) );
    wNrErrorsLeftJoin.setText( Const.NVL( input.getNrErrorsLeftJoinField(), "" ) );
    wNrErrorsInnerJoin.setText( Const.NVL( input.getNrErrorsInnerJoinField(), "" ) );
    wNrErrorsRightJoin.setText( Const.NVL( input.getNrErrorsRightJoinField(), "" ) );

    wKeyDesc.setText( Const.NVL( input.getKeyDescriptionField(), "" ) );
    wReferenceValue.setText( Const.NVL( input.getValueReferenceField(), "" ) );
    wCompareValue.setText( Const.NVL( input.getValueCompareField(), "" ) );

    setComboValues();

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

    input.setReferenceConnection( pipelineMeta.findDatabase( wReferenceDB.getText() ) );
    input.setReferenceSchemaField( wReferenceSchema.getText() );
    input.setReferenceTableField( wReferenceTable.getText() );
    input.setCompareConnection( pipelineMeta.findDatabase( wCompareDB.getText() ) );
    input.setCompareSchemaField( wCompareSchema.getText() );
    input.setCompareTableField( wCompareTable.getText() );
    input.setKeyFieldsField( wKeyFields.getText() );
    input.setExcludeFieldsField( wExcludeFields.getText() );

    input.setNrErrorsField( wNrErrors.getText() );
    input.setNrRecordsReferenceField( wNrRecordsReference.getText() );
    input.setNrRecordsCompareField( wNrRecordsCompare.getText() );
    input.setNrErrorsLeftJoinField( wNrErrorsLeftJoin.getText() );
    input.setNrErrorsInnerJoinField( wNrErrorsInnerJoin.getText() );
    input.setNrErrorsRightJoinField( wNrErrorsRightJoin.getText() );

    input.setKeyDescriptionField( wKeyDesc.getText() );
    input.setValueReferenceField( wReferenceValue.getText() );
    input.setValueCompareField( wCompareValue.getText() );

    dispose();
  }
}
