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

package org.apache.hop.pipeline.transforms.ifnull;


import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IfNullDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = IfNullMeta.class; // For Translator

  private final IfNullMeta input;

  private int fieldsRows = 0;
  private ModifyListener oldlsMod;
  private int margin;

  /**
   * all fields from the previous transforms
   */
  private IRowMeta prevFields = null;

  /**
   * List of ColumnInfo that should have the previous fields combo box
   */
  private final List<ColumnInfo> fieldColumns = new ArrayList<>();

  private Button wSelectFields, wSelectValuesType;

  private Label wlFields, wlValueTypes;
  private TableView wFields, wValueTypes;

  private Label wlReplaceByValue;

  private TextVar wReplaceByValue;

  private Label wlMask;
  private CCombo wMask;

  private Label wlSetEmptyStringAll;
  private Button wSetEmptyStringAll;

  public IfNullDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (IfNullMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();
    oldlsMod = lsMod;
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    int middle = props.getMiddlePct();
    margin = props.getMargin();

    fieldsRows = input.getFields().length;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "IfNullDialog.Shell.Title" ) );

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.addListener( SWT.Selection, e -> get() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "IfNullDialog.TransformName.Label" ) );
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

    // ///////////////////////////////
    // START OF All Fields GROUP //
    // ///////////////////////////////

    Group wAllFields = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wAllFields );
    wAllFields.setText( BaseMessages.getString( PKG, "IfNullDialog.AllFields.Label" ) );

    FormLayout AllFieldsgroupLayout = new FormLayout();
    AllFieldsgroupLayout.marginWidth = 10;
    AllFieldsgroupLayout.marginHeight = 10;
    wAllFields.setLayout( AllFieldsgroupLayout );

    // Replace by Value
    wlReplaceByValue = new Label( wAllFields, SWT.RIGHT );
    wlReplaceByValue.setText( BaseMessages.getString( PKG, "IfNullDialog.ReplaceByValue.Label" ) );
    props.setLook( wlReplaceByValue );
    FormData fdlReplaceByValue = new FormData();
    fdlReplaceByValue.left = new FormAttachment( 0, 0 );
    fdlReplaceByValue.right = new FormAttachment( middle, -margin );
    fdlReplaceByValue.top = new FormAttachment( wTransformName, margin * 2 );
    wlReplaceByValue.setLayoutData( fdlReplaceByValue );

    wReplaceByValue = new TextVar( variables, wAllFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wReplaceByValue.setToolTipText( BaseMessages.getString( PKG, "IfNullDialog.ReplaceByValue.Tooltip" ) );
    props.setLook( wReplaceByValue );
    FormData fdReplaceByValue = new FormData();
    fdReplaceByValue.left = new FormAttachment( middle, 0 );
    fdReplaceByValue.top = new FormAttachment( wTransformName, 2 * margin );
    fdReplaceByValue.right = new FormAttachment( 100, 0 );
    wReplaceByValue.setLayoutData( fdReplaceByValue );

    // SetEmptyStringAll line
    wlSetEmptyStringAll = new Label( wAllFields, SWT.RIGHT );
    wlSetEmptyStringAll.setText( BaseMessages.getString( PKG, "IfNullDialog.SetEmptyStringAll.Label" ) );
    props.setLook( wlSetEmptyStringAll );
    FormData fdlSetEmptyStringAll = new FormData();
    fdlSetEmptyStringAll.left = new FormAttachment( 0, 0 );
    fdlSetEmptyStringAll.top = new FormAttachment( wReplaceByValue, margin );
    fdlSetEmptyStringAll.right = new FormAttachment( middle, -margin );
    wlSetEmptyStringAll.setLayoutData( fdlSetEmptyStringAll );
    wSetEmptyStringAll = new Button( wAllFields, SWT.CHECK );
    wSetEmptyStringAll.setToolTipText( BaseMessages.getString( PKG, "IfNullDialog.SetEmptyStringAll.Tooltip" ) );
    props.setLook( wSetEmptyStringAll );
    FormData fdSetEmptyStringAll = new FormData();
    fdSetEmptyStringAll.left = new FormAttachment( middle, 0 );
    fdSetEmptyStringAll.top = new FormAttachment( wlSetEmptyStringAll, 0, SWT.CENTER );
    fdSetEmptyStringAll.right = new FormAttachment( 100, 0 );
    wSetEmptyStringAll.setLayoutData( fdSetEmptyStringAll );
    wSetEmptyStringAll.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        enableSetEmptyStringAll();
      }
    } );

    wlMask = new Label( wAllFields, SWT.RIGHT );
    wlMask.setText( BaseMessages.getString( PKG, "IfNullDialog.Mask.Label" ) );
    props.setLook( wlMask );
    FormData fdlMask = new FormData();
    fdlMask.left = new FormAttachment( 0, 0 );
    fdlMask.top = new FormAttachment( wSetEmptyStringAll, margin );
    fdlMask.right = new FormAttachment( middle, -margin );
    wlMask.setLayoutData( fdlMask );
    wMask = new CCombo( wAllFields, SWT.BORDER | SWT.READ_ONLY );
    wMask.setEditable( true );
    wMask.setItems( Const.getDateFormats() );
    props.setLook( wMask );
    wMask.addModifyListener( lsMod );
    FormData fdMask = new FormData();
    fdMask.left = new FormAttachment( middle, 0 );
    fdMask.top = new FormAttachment( wSetEmptyStringAll, margin );
    fdMask.right = new FormAttachment( 100, 0 );
    wMask.setLayoutData( fdMask );

    FormData fdAllFields = new FormData();
    fdAllFields.left = new FormAttachment( 0, margin );
    fdAllFields.top = new FormAttachment( wTransformName, margin );
    fdAllFields.right = new FormAttachment( 100, -margin );
    wAllFields.setLayoutData( fdAllFields );

    // ///////////////////////////////////////////////////////////
    // / END OF All Fields GROUP
    // ///////////////////////////////////////////////////////////

    // Select fields?
    Label wlSelectFields = new Label( shell, SWT.RIGHT );
    wlSelectFields.setText( BaseMessages.getString( PKG, "IfNullDialog.SelectFields.Label" ) );
    props.setLook( wlSelectFields );
    FormData fdlSelectFields = new FormData();
    fdlSelectFields.left = new FormAttachment( 0, 0 );
    fdlSelectFields.top = new FormAttachment( wAllFields, margin );
    fdlSelectFields.right = new FormAttachment( middle, -margin );
    wlSelectFields.setLayoutData( fdlSelectFields );
    wSelectFields = new Button( shell, SWT.CHECK );
    wSelectFields.setToolTipText( BaseMessages.getString( PKG, "IfNullDialog.SelectFields.Tooltip" ) );
    props.setLook( wSelectFields );
    FormData fdSelectFields = new FormData();
    fdSelectFields.left = new FormAttachment( middle, 0 );
    fdSelectFields.top = new FormAttachment( wlSelectFields, 0, SWT.CENTER );
    fdSelectFields.right = new FormAttachment( 100, 0 );
    wSelectFields.setLayoutData( fdSelectFields );

    // Select type?
    Label wlSelectValuesType = new Label( shell, SWT.RIGHT );
    wlSelectValuesType.setText( BaseMessages.getString( PKG, "IfNullDialog.SelectValuesType.Label" ) );
    props.setLook( wlSelectValuesType );
    FormData fdlSelectValuesType = new FormData();
    fdlSelectValuesType.left = new FormAttachment( 0, 0 );
    fdlSelectValuesType.top = new FormAttachment( wSelectFields, margin );
    fdlSelectValuesType.right = new FormAttachment( middle, -margin );
    wlSelectValuesType.setLayoutData( fdlSelectValuesType );
    wSelectValuesType = new Button( shell, SWT.CHECK );
    wSelectValuesType.setToolTipText( BaseMessages.getString( PKG, "IfNullDialog.SelectValuesType.Tooltip" ) );
    props.setLook( wSelectValuesType );
    FormData fdSelectValuesType = new FormData();
    fdSelectValuesType.left = new FormAttachment( middle, 0 );
    fdSelectValuesType.top = new FormAttachment( wlSelectValuesType, 0, SWT.CENTER );
    fdSelectValuesType.right = new FormAttachment( 100, 0 );
    wSelectValuesType.setLayoutData( fdSelectValuesType );

    wlValueTypes = new Label( shell, SWT.NONE );
    wlValueTypes.setText( BaseMessages.getString( PKG, "IfNullDialog.ValueTypes.Label" ) );
    props.setLook( wlValueTypes );
    FormData fdlValueTypes = new FormData();
    fdlValueTypes.left = new FormAttachment( 0, 0 );
    fdlValueTypes.top = new FormAttachment( wSelectValuesType, margin );
    wlValueTypes.setLayoutData( fdlValueTypes );

    int valueTypesRows = input.getValueTypes().length;

    ColumnInfo[] colval = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "IfNullDialog.ValueType.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO, IValueMeta.typeCodes ),
      new ColumnInfo( BaseMessages.getString( PKG, "IfNullDialog.Value.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "IfNullDialog.Value.ConversionMask" ), ColumnInfo.COLUMN_TYPE_CCOMBO, Const.getDateFormats() ),
      new ColumnInfo( BaseMessages.getString( PKG, "IfNullDialog.Value.SetEmptyString" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
        BaseMessages.getString( PKG, "System.Combo.Yes" ), BaseMessages.getString( PKG, "System.Combo.No" ) } )
    };
    colval[ 1 ].setUsingVariables( true );

    wValueTypes = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colval, valueTypesRows, oldlsMod, props );
    FormData fdValueTypes = new FormData();
    fdValueTypes.left = new FormAttachment( 0, 0 );
    fdValueTypes.top = new FormAttachment( wlValueTypes, margin );
    fdValueTypes.right = new FormAttachment( 100, 0 );
    fdValueTypes.bottom = new FormAttachment( wlValueTypes, (int) ( 190 * props.getZoomFactor() ) );
    wValueTypes.setLayoutData( fdValueTypes );

    getFirstData();


    addFields();

    wSelectValuesType.addListener( SWT.Selection, e -> {
        activeSelectValuesType();
        input.setChanged();
      }
    );

    wSelectFields.addListener( SWT.Selection, e -> {
      activeSelectFields();
      input.setChanged();
    } );

    // Add listeners

    lsDef = new SelectionAdapter() {
        public void widgetDefaultSelected( SelectionEvent e ) {
          ok();
        }
      }

    ;

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addListener( SWT.Close, e -> cancel() );

    // Set the shell size, based upon previous time...
    setSize();

    getData();

    enableSetEmptyStringAll();

    // setComboValues();
    activeSelectFields();

    activeSelectValuesType();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void addFields() {
    int FieldsCols = 4;
    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];

    // Table with fields
    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "IfNullDialog.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wValueTypes, margin );
    wlFields.setLayoutData( fdlFields );

    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "IfNullDialog.Fieldname.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] {}, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "IfNullDialog.Value.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "IfNullDialog.Value.ConversionMask" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        Const.getDateFormats() );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "IfNullDialog.Value.SetEmptyString" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ), BaseMessages.getString( PKG, "System.Combo.No" ) } );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldsRows, oldlsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2 * margin );

    wFields.setLayoutData( fdFields );

    setComboValues();
    fieldColumns.add( colinf[ 0 ] );

  }

  private void activeSelectFields() {
    if ( wSelectFields.getSelection() ) {
      wSelectValuesType.setSelection( false );
      wlValueTypes.setEnabled( false );
      wValueTypes.setEnabled( false );
    }
    activeFields();
  }

  private void activeSelectValuesType() {
    if ( wSelectValuesType.getSelection() ) {
      wSelectFields.setSelection( false );
      wFields.setEnabled( false );
      wlFields.setEnabled( false );
    }
    activeFields();
  }

  private void activeFields() {
    wlFields.setEnabled( wSelectFields.getSelection() );
    wFields.setEnabled( wSelectFields.getSelection() );
    wGet.setEnabled( wSelectFields.getSelection() );
    wlValueTypes.setEnabled( wSelectValuesType.getSelection() );
    wValueTypes.setEnabled( wSelectValuesType.getSelection() );
    wlReplaceByValue.setEnabled( !wSelectFields.getSelection() && !wSelectValuesType.getSelection() );
    wReplaceByValue.setEnabled( !wSelectFields.getSelection() && !wSelectValuesType.getSelection() );
    wlMask.setEnabled( !wSelectFields.getSelection() && !wSelectValuesType.getSelection() );
    wMask.setEnabled( !wSelectFields.getSelection() && !wSelectValuesType.getSelection() );
    wlSetEmptyStringAll.setEnabled( !wSelectFields.getSelection() && !wSelectValuesType.getSelection() );
    wSetEmptyStringAll.setEnabled( !wSelectFields.getSelection() && !wSelectValuesType.getSelection() );
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener insertListener = ( tableItem, v ) -> true;

        BaseTransformDialog
          .getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, insertListener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  private void setComboValues() {
    Runnable fieldLoader = () -> {
      try {
        prevFields = pipelineMeta.getPrevTransformFields( variables, transformName );

      } catch ( HopException e ) {
        String msg = BaseMessages.getString( PKG, "IfNullDialog.DoMapping.UnableToFindInput" );
        logError( msg );
      }
      String[] prevTransformFieldNames = prevFields.getFieldNames();
      if ( prevTransformFieldNames != null ) {
        Arrays.sort( prevTransformFieldNames );

        for ( ColumnInfo colInfo : fieldColumns ) {
          if ( colInfo != null ) {
            colInfo.setComboValues( prevTransformFieldNames );
          }
        }
      }
    };
    new Thread( fieldLoader ).start();
  }

  public void getFirstData() {
    wSelectFields.setSelection( input.isSelectFields() );
    wSelectValuesType.setSelection( input.isSelectValuesType() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getReplaceAllByValue() != null ) {
      wReplaceByValue.setText( input.getReplaceAllByValue() );
    }
    if ( input.getReplaceAllMask() != null ) {
      wMask.setText( input.getReplaceAllMask() );
    }
    wSetEmptyStringAll.setSelection( input.isSetEmptyStringAll() );

    wSelectFields.setSelection( input.isSelectFields() );
    wSelectValuesType.setSelection( input.isSelectValuesType() );

    Table table = wValueTypes.table;
    if ( input.getValueTypes().length > 0 ) {
      table.removeAll();
    }
    for ( int i = 0; i < input.getValueTypes().length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      if ( input.getValueTypes()[ i ].getTypeName() != null ) {
        ti.setText( 1, input.getValueTypes()[ i ].getTypeName() );
      }
      if ( input.getValueTypes()[ i ].getTypereplaceValue() != null ) {
        ti.setText( 2, input.getValueTypes()[ i ].getTypereplaceValue() );
      }
      if ( input.getValueTypes()[ i ].getTypereplaceMask() != null ) {
        ti.setText( 3, input.getValueTypes()[ i ].getTypereplaceMask() );
      }
      ti.setText( 4, input.getValueTypes()[ i ].isSetTypeEmptyString() ? BaseMessages.getString( PKG, "System.Combo.Yes" )
        : BaseMessages.getString( PKG, "System.Combo.No" ) );

    }

    wValueTypes.setRowNums();
    wValueTypes.removeEmptyRows();
    wValueTypes.optWidth( true );

    table = wFields.table;
    if ( input.getFields().length > 0 ) {
      table.removeAll();
    }
    for ( int i = 0; i < input.getFields().length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      if ( input.getFields()[ i ].getFieldName() != null ) {
        ti.setText( 1, input.getFields()[ i ].getFieldName() );
      }
      if ( input.getFields()[ i ].getReplaceValue() != null ) {
        ti.setText( 2, input.getFields()[ i ].getReplaceValue() );
      }
      if ( input.getFields()[ i ].getReplaceMask() != null ) {
        ti.setText( 3, input.getFields()[ i ].getReplaceMask() );
      }
      ti.setText( 4, input.getFields()[ i ].isSetEmptyString()
        ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString( PKG, "System.Combo.No" ) );
    }

    wFields.setRowNums();
    wValueTypes.removeEmptyRows();
    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void enableSetEmptyStringAll() {
    wMask.setText( "" );
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    transformName = wTransformName.getText(); // return value

    input.setEmptyStringAll( wSetEmptyStringAll.getSelection() );

    if ( wSetEmptyStringAll.getSelection() ) {
      input.setReplaceAllByValue( "" );
      input.setReplaceAllMask( "" );

    } else {
      input.setReplaceAllByValue( wReplaceByValue.getText() );
      input.setReplaceAllMask( wMask.getText() );
    }

    input.setSelectFields( wSelectFields.getSelection() );
    input.setSelectValuesType( wSelectValuesType.getSelection() );

    int nrtypes = wValueTypes.nrNonEmpty();
    int nrFields = wFields.nrNonEmpty();
    input.allocate( nrtypes, nrFields );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrtypes; i++ ) {
      TableItem ti = wValueTypes.getNonEmpty( i );
      input.getValueTypes()[ i ].setTypeName( ti.getText( 1 ) );
      input.getValueTypes()[ i ].setTypeEmptyString( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase(
        ti.getText( 4 ) ) );
      if ( input.getValueTypes()[ i ].isSetTypeEmptyString() ) {
        input.getValueTypes()[ i ].setTypereplaceValue( "" );
        input.getValueTypes()[ i ].setTypereplaceMask( "" );
      } else {
        input.getValueTypes()[ i ].setTypereplaceValue( ti.getText( 2 ) );
        input.getValueTypes()[ i ].setTypereplaceMask( ti.getText( 3 ) );
      }
    }

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem ti = wFields.getNonEmpty( i );
      input.getFields()[ i ].setFieldName( ti.getText( 1 ) );
      input.getFields()[ i ].setEmptyString( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( ti
        .getText( 4 ) ) );
      if ( input.getFields()[ i ].isSetEmptyString() ) {
        input.getFields()[ i ].setReplaceValue( "" );
        input.getFields()[ i ].setReplaceMask( "" );
      } else {
        input.getFields()[ i ].setReplaceValue( ti.getText( 2 ) );
        input.getFields()[ i ].setReplaceMask( ti.getText( 3 ) );
      }
    }
    dispose();
  }
}
