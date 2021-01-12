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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.apache.hop.core.variables.IVariables;

public class SwitchCaseDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SwitchCaseMeta.class; // For Translator

  private CCombo wFieldName;

  private CCombo wDataType;

  private Text wConversionMask;

  private Text wDecimalSymbol;

  private Text wGroupingSymbol;

  private TableView wValues;

  private CCombo wDefaultTarget;

  private Button wContains;

  private final SwitchCaseMeta input;

  public SwitchCaseDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (SwitchCaseMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    SelectionAdapter lsSel = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, margin );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // The name of the field to validate
    //
    Label wlFieldName = new Label(shell, SWT.RIGHT);
    wlFieldName.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.FieldName.Label" ) );
    props.setLook(wlFieldName);
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment( 0, 0 );
    fdlFieldName.right = new FormAttachment( middle, 0 );
    fdlFieldName.top = new FormAttachment( wTransformName, margin );
    wlFieldName.setLayoutData( fdlFieldName );
    wFieldName = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFieldName );
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment( middle, margin );
    fdFieldName.right = new FormAttachment( 100, 0 );
    fdFieldName.top = new FormAttachment( wTransformName, margin );
    wFieldName.setLayoutData( fdFieldName );
    wFieldName.addModifyListener( lsMod );

    // TODO: grab field list in thread in the background...
    //
    try {
      IRowMeta inputFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      wFieldName.setItems( inputFields.getFieldNames() );
    } catch ( HopTransformException ex ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SwitchCaseDialog.Exception.CantGetFieldsFromPreviousTransforms.Title" ),
        BaseMessages.getString( PKG, "SwitchCaseDialog.Exception.CantGetFieldsFromPreviousTransforms.Message" ), ex );
    }

    Label wlContains = new Label(shell, SWT.RIGHT);
    wlContains.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.Contains.Label" ) );
    props.setLook(wlContains);
    FormData fdlContains = new FormData();
    fdlContains.left = new FormAttachment( 0, 0 );
    fdlContains.right = new FormAttachment( middle, 0 );
    fdlContains.top = new FormAttachment( wFieldName, margin * 2 );
    wlContains.setLayoutData(fdlContains);
    wContains = new Button( shell, SWT.CHECK );
    wContains.setToolTipText( BaseMessages.getString( PKG, "SwitchCaseDialog.Contains.Tooltip" ) );
    props.setLook( wContains );
    FormData fdContains = new FormData();
    fdContains.left = new FormAttachment( middle, margin );
    fdContains.top = new FormAttachment( wlContains, 0, SWT.CENTER );
    fdContains.right = new FormAttachment( 100, 0 );
    wContains.setLayoutData(fdContains);
    wContains.addSelectionListener( lsSel );

    // Data type
    //
    Label wlDataType = new Label(shell, SWT.RIGHT);
    wlDataType.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.DataType.Label" ) );
    props.setLook(wlDataType);
    FormData fdlDataType = new FormData();
    fdlDataType.left = new FormAttachment( 0, 0 );
    fdlDataType.right = new FormAttachment( middle, 0 );
    fdlDataType.top = new FormAttachment( wContains, margin );
    wlDataType.setLayoutData( fdlDataType );
    wDataType = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDataType.setItems( ValueMetaBase.getTypes() );
    props.setLook( wDataType );
    FormData fdDataType = new FormData();
    fdDataType.left = new FormAttachment( middle, margin );
    fdDataType.right = new FormAttachment( 100, 0 );
    fdDataType.top = new FormAttachment( wContains, margin );
    wDataType.setLayoutData( fdDataType );
    wDataType.addModifyListener( lsMod );

    // Conversion mask
    //
    Label wlConversionMask = new Label(shell, SWT.RIGHT);
    wlConversionMask.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.ConversionMask.Label" ) );
    props.setLook(wlConversionMask);
    FormData fdlConversionMask = new FormData();
    fdlConversionMask.left = new FormAttachment( 0, 0 );
    fdlConversionMask.right = new FormAttachment( middle, 0 );
    fdlConversionMask.top = new FormAttachment( wDataType, margin );
    wlConversionMask.setLayoutData( fdlConversionMask );
    wConversionMask = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConversionMask );
    FormData fdConversionMask = new FormData();
    fdConversionMask.left = new FormAttachment( middle, margin );
    fdConversionMask.right = new FormAttachment( 100, 0 );
    fdConversionMask.top = new FormAttachment( wDataType, margin );
    wConversionMask.setLayoutData( fdConversionMask );
    wConversionMask.addModifyListener( lsMod );

    // Decimal Symbol
    //
    Label wlDecimalSymbol = new Label(shell, SWT.RIGHT);
    wlDecimalSymbol.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.DecimalSymbol.Label" ) );
    props.setLook(wlDecimalSymbol);
    FormData fdlDecimalSymbol = new FormData();
    fdlDecimalSymbol.left = new FormAttachment( 0, 0 );
    fdlDecimalSymbol.right = new FormAttachment( middle, 0 );
    fdlDecimalSymbol.top = new FormAttachment( wConversionMask, margin );
    wlDecimalSymbol.setLayoutData( fdlDecimalSymbol );
    wDecimalSymbol = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDecimalSymbol );
    FormData fdDecimalSymbol = new FormData();
    fdDecimalSymbol.left = new FormAttachment( middle, margin );
    fdDecimalSymbol.right = new FormAttachment( 100, 0 );
    fdDecimalSymbol.top = new FormAttachment( wConversionMask, margin );
    wDecimalSymbol.setLayoutData( fdDecimalSymbol );
    wDecimalSymbol.addModifyListener( lsMod );

    // Grouping Symbol
    //
    Label wlGroupingSymbol = new Label(shell, SWT.RIGHT);
    wlGroupingSymbol.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.GroupingSymbol.Label" ) );
    props.setLook(wlGroupingSymbol);
    FormData fdlGroupingSymbol = new FormData();
    fdlGroupingSymbol.left = new FormAttachment( 0, 0 );
    fdlGroupingSymbol.right = new FormAttachment( middle, 0 );
    fdlGroupingSymbol.top = new FormAttachment( wDecimalSymbol, margin );
    wlGroupingSymbol.setLayoutData( fdlGroupingSymbol );
    wGroupingSymbol = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGroupingSymbol );
    FormData fdGroupingSymbol = new FormData();
    fdGroupingSymbol.left = new FormAttachment( middle, margin );
    fdGroupingSymbol.right = new FormAttachment( 100, 0 );
    fdGroupingSymbol.top = new FormAttachment( wDecimalSymbol, margin );
    wGroupingSymbol.setLayoutData( fdGroupingSymbol );
    wGroupingSymbol.addModifyListener( lsMod );

    String[] nextTransformNames = pipelineMeta.getNextTransformNames( transformMeta );

    // The values to switch on...
    //
    Label wlValues = new Label(shell, SWT.RIGHT);
    wlValues.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.ValueCases.Label" ) );
    props.setLook(wlValues);
    FormData fdlValues = new FormData();
    fdlValues.left = new FormAttachment( 0, 0 );
    fdlValues.top = new FormAttachment( wGroupingSymbol, margin );
    fdlValues.right = new FormAttachment( middle, 0 );
    wlValues.setLayoutData( fdlValues );

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "SwitchCaseDialog.ColumnInfo.Value" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SwitchCaseDialog.ColumnInfo.TargetTransform" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, nextTransformNames, false ), };

    wValues =
      new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, input
        .getTransformIOMeta().getTargetStreams().size(), lsMod, props );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // The name of the field to validate
    //
    Label wlDefaultTarget = new Label(shell, SWT.RIGHT);
    wlDefaultTarget.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.DefaultTarget.Label" ) );
    props.setLook(wlDefaultTarget);
    FormData fdlDefaultTarget = new FormData();
    fdlDefaultTarget.left = new FormAttachment( 0, 0 );
    fdlDefaultTarget.right = new FormAttachment( middle, 0 );
    fdlDefaultTarget.bottom = new FormAttachment( wOk, -margin * 2 );
    wlDefaultTarget.setLayoutData( fdlDefaultTarget );
    wDefaultTarget = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDefaultTarget );
    FormData fdDefaultTarget = new FormData();
    fdDefaultTarget.left = new FormAttachment( middle, margin );
    fdDefaultTarget.right = new FormAttachment( 100, 0 );
    fdDefaultTarget.bottom = new FormAttachment( wOk, -margin * 2 );
    wDefaultTarget.setLayoutData( fdDefaultTarget );
    wDefaultTarget.setItems( nextTransformNames );
    wDefaultTarget.addModifyListener( lsMod );

    FormData fdValues = new FormData();
    fdValues.left = new FormAttachment( middle, margin );
    fdValues.top = new FormAttachment( wGroupingSymbol, margin );
    fdValues.right = new FormAttachment( 100, 0 );
    fdValues.bottom = new FormAttachment( wDefaultTarget, -margin );
    wValues.setLayoutData( fdValues );

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

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wFieldName.setText( Const.NVL( input.getFieldname(), "" ) );
    wContains.setSelection( input.isContains() );
    wDataType.setText( ValueMetaBase.getTypeDesc( input.getCaseValueType() ) );
    wDecimalSymbol.setText( Const.NVL( input.getCaseValueDecimal(), "" ) );
    wGroupingSymbol.setText( Const.NVL( input.getCaseValueGroup(), "" ) );
    wConversionMask.setText( Const.NVL( input.getCaseValueFormat(), "" ) );

    for ( int i = 0; i < input.getCaseTargets().size(); i++ ) {
      TableItem item = wValues.table.getItem( i );
      SwitchCaseTarget target = input.getCaseTargets().get( i );
      if ( target != null ) {
        item.setText( 1, Const.NVL( target.caseValue, "" ) ); // The value
        item.setText( 2, target.caseTargetTransform == null ? "" : target.caseTargetTransform.getName() ); // The target transform name
      }
    }
    wValues.removeEmptyRows();
    wValues.setRowNums();
    wValues.optWidth( true );

    wDefaultTarget.setText( input.getDefaultTargetTransform() == null ? "" : input.getDefaultTargetTransform().getName() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    input.setFieldname( wFieldName.getText() );
    input.setContains( wContains.getSelection() );
    input.setCaseValueType( ValueMetaBase.getType( wDataType.getText() ) );
    input.setCaseValueFormat( wConversionMask.getText() );
    input.setCaseValueDecimal( wDecimalSymbol.getText() );
    input.setCaseValueGroup( wGroupingSymbol.getText() );

    int nrValues = wValues.nrNonEmpty();
    input.allocate();

    for ( int i = 0; i < nrValues; i++ ) {
      TableItem item = wValues.getNonEmpty( i );

      SwitchCaseTarget target = new SwitchCaseTarget();
      target.caseValue = item.getText( 1 );
      target.caseTargetTransform = pipelineMeta.findTransform( item.getText( 2 ) );
      input.getCaseTargets().add( target );
    }

    input.setDefaultTargetTransform( pipelineMeta.findTransform( wDefaultTarget.getText() ) );

    transformName = wTransformName.getText(); // return value

    dispose();
  }
}
