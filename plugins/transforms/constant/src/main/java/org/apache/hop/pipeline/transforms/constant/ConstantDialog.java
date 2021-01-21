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

package org.apache.hop.pipeline.transforms.constant;


import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ConstantDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ConstantMeta.class; // For Translator

  private TableView wFields;

  private final ConstantMeta input;

  public ConstantDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ConstantMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "ConstantDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // The buttons at the bottom of the dialog
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e->ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // Filename line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
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

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "ConstantDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wTransformName, margin );
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 10;
    final int FieldsRows = input.getFieldName().length;

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Name.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Type.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        ValueMetaFactory.getValueMetaNames() );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Format.Column" ), ColumnInfo.COLUMN_TYPE_FORMAT, 2 );
    colinf[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Length.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 4 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Precision.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 5 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Currency.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 6 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Decimal.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 7 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Group.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 8 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Value.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 9 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConstantDialog.Value.SetEmptyString" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ), BaseMessages.getString( PKG, "System.Combo.No" ) } );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2*margin );
    wFields.setLayoutData(fdFields);




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

    lsResize = event -> {
      Point size = shell.getSize();
      wFields.setSize( size.x - 10, size.y - 50 );
      wFields.table.setSize( size.x - 10, size.y - 50 );
      wFields.redraw();
    };
    shell.addListener( SWT.Resize, lsResize );

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
    int i;
    if ( log.isDebug() ) {
      logDebug( "getting fields info..." );
    }

    for ( i = 0; i < input.getFieldName().length; i++ ) {
      if ( input.getFieldName()[ i ] != null ) {
        TableItem item = wFields.table.getItem( i );
        int col = 1;
        item.setText( col++, input.getFieldName()[ i ] );

        String type = input.getFieldType()[ i ];
        String format = input.getFieldFormat()[ i ];
        String length = input.getFieldLength()[ i ] < 0 ? "" : ( "" + input.getFieldLength()[ i ] );
        String prec = input.getFieldPrecision()[ i ] < 0 ? "" : ( "" + input.getFieldPrecision()[ i ] );

        String curr = input.getCurrency()[ i ];
        String group = input.getGroup()[ i ];
        String decim = input.getDecimal()[ i ];
        String def = input.getValue()[ i ];

        item.setText( col++, Const.NVL( type, "" ) );
        item.setText( col++, Const.NVL( format, "" ) );
        item.setText( col++, Const.NVL( length, "" ) );
        item.setText( col++, Const.NVL( prec, "" ) );
        item.setText( col++, Const.NVL( curr, "" ) );
        item.setText( col++, Const.NVL( decim, "" ) );
        item.setText( col++, Const.NVL( group, "" ) );
        item.setText( col++, Const.NVL( def, "" ) );
        item
          .setText( col++, input.isSetEmptyString()[ i ]
            ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" ) );

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

    int i;
    // Table table = wFields.table;

    int nrFields = wFields.nrNonEmpty();

    input.allocate( nrFields );

    //CHECKSTYLE:Indentation:OFF
    //CHECKSTYLE:LineLength:OFF
    for ( i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getFieldName()[ i ] = item.getText( 1 );
      input.isSetEmptyString()[ i ] = BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( 10 ) );

      input.getFieldType()[ i ] = input.isSetEmptyString()[ i ] ? "String" : item.getText( 2 );
      input.getFieldFormat()[ i ] = item.getText( 3 );
      String slength = item.getText( 4 );
      String sprec = item.getText( 5 );
      input.getCurrency()[ i ] = item.getText( 6 );
      input.getDecimal()[ i ] = item.getText( 7 );
      input.getGroup()[ i ] = item.getText( 8 );
      input.getValue()[ i ] = input.isSetEmptyString()[ i ] ? "" : item.getText( 9 );

      try {
        input.getFieldLength()[ i ] = Integer.parseInt( slength );
      } catch ( Exception e ) {
        input.getFieldLength()[ i ] = -1;
      }
      try {
        input.getFieldPrecision()[ i ] = Integer.parseInt( sprec );
      } catch ( Exception e ) {
        input.getFieldPrecision()[ i ] = -1;
      }

    }

    dispose();
  }
}
