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

package org.apache.hop.pipeline.transforms.getvariable;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.getvariable.GetVariableMeta.FieldDefinition;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class GetVariableDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = GetVariableMeta.class; // For Translator

  private Text wTransformName;

  private TableView wFields;

  private final GetVariableMeta input;

  public GetVariableDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (GetVariableMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "GetVariableDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // See if the transform receives input.
    //
    boolean isReceivingInput = pipelineMeta.findNrPrevTransforms(transformMeta) > 0;

    // TransformName line
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData(fdTransformName);

    // Some buttons at the bottom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wGet = new Button( this.shell, 8 );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetVariables" ) );
    wGet.addListener( 13, e -> grabVariables() );
    wPreview = new Button( this.shell, 8 );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wPreview.setEnabled( !isReceivingInput);
    wPreview.addListener( 13, e -> preview() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wPreview, wGet, wCancel }, margin, null );

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "GetVariableDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wTransformName, margin );
    wlFields.setLayoutData(fdlFields);

    final int fieldsRows = input.getFieldDefinitions().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetVariableDialog.NameColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetVariableDialog.VariableColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "System.Column.Type" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "System.Column.Format" ), ColumnInfo.COLUMN_TYPE_FORMAT, 3 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "System.Column.Length" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "System.Column.Precision" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "System.Column.Currency" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "System.Column.Decimal" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "System.Column.Group" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "GetVariableDialog.TrimType.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMetaString.getTrimTypeDescriptions() ), };

    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "GetVariableDialog.VariableColumn.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2*margin );
    wFields.setLayoutData(fdFields);

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

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wTransformName.setText( transformName );

    for ( int i = 0; i < input.getFieldDefinitions().length; i++ ) {
      TableItem item = wFields.table.getItem( i );

      int index = 1;
      FieldDefinition currentField = input.getFieldDefinitions()[ i ];
      item.setText( index++, Const.NVL( currentField.getFieldName(), "" ) );
      item.setText( index++, Const.NVL( currentField.getVariableString(), "" ) );
      item.setText( index++, ValueMetaFactory.getValueMetaName( currentField.getFieldType() ) );
      item.setText( index++, Const.NVL( currentField.getFieldFormat(), "" ) );
      item.setText( index++, currentField.getFieldLength() < 0 ? "" : ( "" + currentField.getFieldLength() ) );
      item.setText( index++, currentField.getFieldPrecision() < 0 ? "" : ( "" + currentField.getFieldPrecision() ) );
      item.setText( index++, Const.NVL( currentField.getCurrency(), "" ) );
      item.setText( index++, Const.NVL( currentField.getDecimal(), "" ) );
      item.setText( index++, Const.NVL( currentField.getGroup(), "" ) );
      item.setText( index++, ValueMetaString.getTrimTypeDesc( currentField.getTrimType() ) );
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

  private void getInfo( GetVariableMeta input ) throws HopException {

    transformName = wTransformName.getText(); // return value
    // Table table = wFields.table;

    int count = wFields.nrNonEmpty();
    input.allocate( count );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < count; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      FieldDefinition currentField = input.getFieldDefinitions()[ i ];
      int index = 1;
      currentField.setFieldName( item.getText( index++ ) );
      currentField.setVariableString( item.getText( index++ ) );
      currentField.setFieldType( ValueMetaFactory.getIdForValueMeta( item.getText( index++ ) ) );
      currentField.setFieldFormat( item.getText( index++ ) );
      currentField.setFieldLength( Const.toInt( item.getText( index++ ), -1 ) );
      currentField.setFieldPrecision( Const.toInt( item.getText( index++ ), -1 ) );
      currentField.setCurrency( item.getText( index++ ) );
      currentField.setDecimal( item.getText( index++ ) );
      currentField.setGroup( item.getText( index++ ) );
      currentField.setTrimType( ValueMetaString.getTrimTypeByDesc( item.getText( index++ ) ) );
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    try {
      getInfo( input );
    } catch ( HopException e ) {
      new ErrorDialog( shell, "Error", "Error saving transform information", e );
    }
    dispose();
  }

  // Preview the data
  private void preview() {
    try {
      // Create the Access input transform
      GetVariableMeta oneMeta = new GetVariableMeta();
      getInfo( oneMeta );

      PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(),
        oneMeta, wTransformName.getText() );

      // We always just want to preview a single output row
      //
      PipelinePreviewProgressDialog progressDialog = new PipelinePreviewProgressDialog( shell, variables, previewMeta, new String[] { wTransformName.getText() }, new int[] { 1 } );
      progressDialog.open();

      if ( !progressDialog.isCancelled() ) {
        Pipeline pipeline = progressDialog.getPipeline();
        String loggingText = progressDialog.getLoggingText();

        if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog(
              shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
              .getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }

        PreviewRowsDialog prd =
          new PreviewRowsDialog(
            shell, variables, SWT.NONE, wTransformName.getText(), progressDialog.getPreviewRowsMeta( wTransformName
            .getText() ), progressDialog.getPreviewRows( wTransformName.getText() ), loggingText );
        prd.open();

      }

    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "GetVariableDialog.ErrorPreviewingData.DialogTitle" ), BaseMessages
        .getString( PKG, "GetVariableDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }

  private void grabVariables() {

    if ( pipelineMeta == null ) {
      return;
    }
    String[] key = variables.getVariableNames();
    int size = key.length;
    String[] val = new String[ size ];
    wFields.removeAll();

    for ( int i = 0; i < size; i++ ) {
      val[ i ] = variables.resolve( key[ i ] );
      TableItem tableItem = new TableItem( wFields.table, 0 );
      tableItem.setText( 1, key[ i ] );
      tableItem.setText( 2, "${" + key[ i ] + "}" );
      tableItem.setText( 3, "String" );
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );
  }
}
