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

package org.apache.hop.pipeline.transforms.calculator;


import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;


public class CalculatorDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = CalculatorMeta.class; // For Translator

  private Text wTransformName;

  private Button wFailIfNoFile;

  private TableView wFields;

  private final CalculatorMeta currentMeta;
  private final CalculatorMeta originalMeta;

  private final Map<String, Integer> inputFields;
  private ColumnInfo[] colinf;

  public CalculatorDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );

    // The order here is important... currentMeta is looked at for changes
    currentMeta = (CalculatorMeta) in;
    originalMeta = (CalculatorMeta) currentMeta.clone();
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, currentMeta );

    ModifyListener lsMod = e -> currentMeta.setChanged();
    changed = currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    int formMargin = 15;
    formLayout.marginWidth = formMargin;
    formLayout.marginHeight = formMargin;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CalculatorDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();
    int fdMargin = 15;

    // Buttons at the very bottom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection,  e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    positionBottomButtons( shell, new Button[] { wOk, wCancel }, fdMargin, null );

    // TransformName line
    Label wlTransformName = new Label(shell, SWT.LEFT);
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, 0 );
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( 0, 0 );
    fdTransformName.top = new FormAttachment(wlTransformName, margin );
    fdTransformName.right = new FormAttachment( middle, 0 );
    wTransformName.setLayoutData(fdTransformName);

    //Image
    Label wIcon = new Label( shell, SWT.RIGHT );
    wIcon.setImage( getImage() );
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment( 0, 0 );
    fdlIcon.right = new FormAttachment( 100, 0 );
    wIcon.setLayoutData( fdlIcon );
    props.setLook( wIcon );

    // Draw line separator
    Label separator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment( 0, 0 );
    fdSeparator.top = new FormAttachment( wTransformName, fdMargin );
    fdSeparator.right = new FormAttachment( 100, 0 );
    separator.setLayoutData( fdSeparator );

    // Fail if no File line
    wFailIfNoFile = new Button( shell, SWT.CHECK );
    props.setLook( wFailIfNoFile );
    wFailIfNoFile.setToolTipText( BaseMessages.getString( PKG, "CalculatorDialog.FailIfNoFileTooltip" ) );
    wFailIfNoFile.setText( BaseMessages.getString( PKG, "CalculatorDialog.FailIfNoFile" ) );
    FormData fdFailIfNoFile = new FormData();
    fdFailIfNoFile.left = new FormAttachment( 0, 0 );
    fdFailIfNoFile.top = new FormAttachment( separator, fdMargin );
    wFailIfNoFile.setLayoutData(fdFailIfNoFile);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "CalculatorDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wFailIfNoFile, fdMargin );
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = currentMeta.getCalculation() != null ? currentMeta.getCalculation().length : 1;

    colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.NewFieldColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.CalculationColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.FieldAColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.FieldBColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.FieldCColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.ValueTypeColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.LengthColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.PrecisionColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.RemoveColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
          BaseMessages.getString( PKG, "System.Combo.No" ),
          BaseMessages.getString( PKG, "System.Combo.Yes" ) } ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.ConversionMask.Column" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 6 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.DecimalSymbol.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.GroupingSymbol.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "CalculatorDialog.CurrencySymbol.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    colinf[ 1 ].setSelectionAdapter( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        EnterSelectionDialog esd =
          new EnterSelectionDialog( shell, CalculatorMetaFunction.calcLongDesc,
            BaseMessages.getString( PKG, "CalculatorDialog.SelectCalculationType.Title" ),
            BaseMessages.getString( PKG, "CalculatorDialog.SelectCalculationType.Message" ) );
        String string = esd.open();
        if ( string != null ) {
          TableView tv = (TableView) e.widget;
          tv.setText( string, e.x, e.y );
          currentMeta.setChanged();
        }
      }
    } );




    // Draw line separator
    Label hSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSeparator = new FormData();
    fdhSeparator.left = new FormAttachment( 0, 0 );
    fdhSeparator.right = new FormAttachment( 100, 0 );
    fdhSeparator.bottom = new FormAttachment( wOk, -fdMargin );
    hSeparator.setLayoutData( fdhSeparator );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( hSeparator, -fdMargin );
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background
    //
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
          logError( BaseMessages.getString( PKG, "CalculatorDialog.Log.UnableToFindInput" ) );
        }
      }
    };
    new Thread( runnable ).start();

    wFields.addModifyListener( arg0 -> {
      // Now set the combo's
      shell.getDisplay().asyncExec( () -> setComboBoxes() );

    } );

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

    getData();
    currentMeta.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    shell.getDisplay().syncExec( () -> {
      // Add the newly create fields.
      //
      int nrNonEmptyFields = wFields.nrNonEmpty();
      for ( int i = 0; i < nrNonEmptyFields; i++ ) {
        TableItem item = wFields.getNonEmpty( i );
        fields.put( item.getText( 1 ), 1000000 + i); // The number is just to debug the origin of
        // the fieldname
      }
    } );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    colinf[ 2 ].setComboValues( fieldNames );
    colinf[ 3 ].setComboValues( fieldNames );
    colinf[ 4 ].setComboValues( fieldNames );
  }

  /**
   * Copy information from the meta-data currentMeta to the dialog fields.
   */
  public void getData() {
    if ( currentMeta.getCalculation() != null ) {
      for ( int i = 0; i < currentMeta.getCalculation().length; i++ ) {
        CalculatorMetaFunction fn = currentMeta.getCalculation()[ i ];
        TableItem item = wFields.table.getItem( i );
        item.setText( 1, Const.NVL( fn.getFieldName(), "" ) );
        item.setText( 2, Const.NVL( fn.getCalcTypeLongDesc(), "" ) );
        item.setText( 3, Const.NVL( fn.getFieldA(), "" ) );
        item.setText( 4, Const.NVL( fn.getFieldB(), "" ) );
        item.setText( 5, Const.NVL( fn.getFieldC(), "" ) );
        item.setText( 6, Const.NVL( ValueMetaFactory.getValueMetaName( fn.getValueType() ), "" ) );
        if ( fn.getValueLength() >= 0 ) {
          item.setText( 7, "" + fn.getValueLength() );
        }
        if ( fn.getValuePrecision() >= 0 ) {
          item.setText( 8, "" + fn.getValuePrecision() );
        }
        item
          .setText( 9, fn.isRemovedFromResult()
            ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" ) );
        item.setText( 10, Const.NVL( fn.getConversionMask(), "" ) );
        item.setText( 11, Const.NVL( fn.getDecimalSymbol(), "" ) );
        item.setText( 12, Const.NVL( fn.getGroupingSymbol(), "" ) );
        item.setText( 13, Const.NVL( fn.getCurrencySymbol(), "" ) );
      }
    }

    wFailIfNoFile.setSelection( currentMeta.isFailIfNoFile() );

    wFields.setRowNums();
    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    currentMeta.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    currentMeta.setFailIfNoFile( wFailIfNoFile.getSelection() );

    int nrNonEmptyFields = wFields.nrNonEmpty();
    currentMeta.allocate( nrNonEmptyFields );

    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      String fieldName = item.getText( 1 );
      int calcType = CalculatorMetaFunction.getCalcFunctionType( item.getText( 2 ) );
      String fieldA = item.getText( 3 );
      String fieldB = item.getText( 4 );
      String fieldC = item.getText( 5 );
      int valueType = ValueMetaFactory.getIdForValueMeta( item.getText( 6 ) );
      int valueLength = Const.toInt( item.getText( 7 ), -1 );
      int valuePrecision = Const.toInt( item.getText( 8 ), -1 );
      boolean removed = BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( 9 ) );
      String conversionMask = item.getText( 10 );
      String decimalSymbol = item.getText( 11 );
      String groupingSymbol = item.getText( 12 );
      String currencySymbol = item.getText( 13 );

      //CHECKSTYLE:Indentation:OFF
      currentMeta.getCalculation()[ i ] = new CalculatorMetaFunction(
        fieldName, calcType, fieldA, fieldB, fieldC, valueType, valueLength, valuePrecision, removed,
        conversionMask, decimalSymbol, groupingSymbol, currencySymbol );
    }

    if ( !originalMeta.equals( currentMeta ) ) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }

  protected Image getImage() {
    return SwtSvgImageUtil
      .getImage( shell.getDisplay(), getClass().getClassLoader(), "calculator.svg", ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE );
  }
}
