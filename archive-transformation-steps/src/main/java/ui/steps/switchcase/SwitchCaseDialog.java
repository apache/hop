/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.trans.steps.switchcase;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.switchcase.SwitchCaseMeta;
import org.apache.hop.trans.steps.switchcase.SwitchCaseTarget;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SwitchCaseDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = SwitchCaseMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlFieldName;
  private CCombo wFieldName;

  private Label wlDataType;
  private CCombo wDataType;

  private Label wlConversionMask;
  private Text wConversionMask;

  private Label wlDecimalSymbol;
  private Text wDecimalSymbol;

  private Label wlGroupingSymbol;
  private Text wGroupingSymbol;

  private Label wlValues;
  private TableView wValues;

  private Label wlDefaultTarget;
  private CCombo wDefaultTarget;

  private Label wlContains;
  private Button wContains;
  private FormData fdlContains, fdContains;

  private SwitchCaseMeta input;
  private RowMetaInterface inputFields;

  public SwitchCaseDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (SwitchCaseMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
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

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, margin );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // The name of the field to validate
    //
    wlFieldName = new Label( shell, SWT.RIGHT );
    wlFieldName.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.FieldName.Label" ) );
    props.setLook( wlFieldName );
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment( 0, 0 );
    fdlFieldName.right = new FormAttachment( middle, 0 );
    fdlFieldName.top = new FormAttachment( wStepname, margin );
    wlFieldName.setLayoutData( fdlFieldName );
    wFieldName = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFieldName );
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment( middle, margin );
    fdFieldName.right = new FormAttachment( 100, 0 );
    fdFieldName.top = new FormAttachment( wStepname, margin );
    wFieldName.setLayoutData( fdFieldName );
    wFieldName.addModifyListener( lsMod );

    // TODO: grab field list in thread in the background...
    //
    try {
      inputFields = transMeta.getPrevStepFields( stepMeta );
      wFieldName.setItems( inputFields.getFieldNames() );
    } catch ( HopStepException ex ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SwitchCaseDialog.Exception.CantGetFieldsFromPreviousSteps.Title" ),
        BaseMessages.getString( PKG, "SwitchCaseDialog.Exception.CantGetFieldsFromPreviousSteps.Message" ), ex );
    }

    wlContains = new Label( shell, SWT.RIGHT );
    wlContains.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.Contains.Label" ) );
    props.setLook( wlContains );
    fdlContains = new FormData();
    fdlContains.left = new FormAttachment( 0, 0 );
    fdlContains.right = new FormAttachment( middle, -margin );
    fdlContains.top = new FormAttachment( wFieldName, margin * 2 );
    wlContains.setLayoutData( fdlContains );
    wContains = new Button( shell, SWT.CHECK );
    wContains.setToolTipText( BaseMessages.getString( PKG, "SwitchCaseDialog.Contains.Tooltip" ) );
    props.setLook( wContains );
    fdContains = new FormData();
    fdContains.left = new FormAttachment( middle, 0 );
    fdContains.top = new FormAttachment( wFieldName, margin * 2 );
    fdContains.right = new FormAttachment( 100, 0 );
    wContains.setLayoutData( fdContains );
    wContains.addSelectionListener( lsSel );

    // Data type
    //
    wlDataType = new Label( shell, SWT.RIGHT );
    wlDataType.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.DataType.Label" ) );
    props.setLook( wlDataType );
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
    wlConversionMask = new Label( shell, SWT.RIGHT );
    wlConversionMask.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.ConversionMask.Label" ) );
    props.setLook( wlConversionMask );
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
    wlDecimalSymbol = new Label( shell, SWT.RIGHT );
    wlDecimalSymbol.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.DecimalSymbol.Label" ) );
    props.setLook( wlDecimalSymbol );
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
    wlGroupingSymbol = new Label( shell, SWT.RIGHT );
    wlGroupingSymbol.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.GroupingSymbol.Label" ) );
    props.setLook( wlGroupingSymbol );
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

    String[] nextStepNames = transMeta.getNextStepNames( stepMeta );

    // The values to switch on...
    //
    wlValues = new Label( shell, SWT.RIGHT );
    wlValues.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.ValueCases.Label" ) );
    props.setLook( wlValues );
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
          BaseMessages.getString( PKG, "SwitchCaseDialog.ColumnInfo.TargetStep" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, nextStepNames, false ), };

    wValues =
      new TableView( transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, input
        .getStepIOMeta().getTargetStreams().size(), lsMod, props );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    // The name of the field to validate
    //
    wlDefaultTarget = new Label( shell, SWT.RIGHT );
    wlDefaultTarget.setText( BaseMessages.getString( PKG, "SwitchCaseDialog.DefaultTarget.Label" ) );
    props.setLook( wlDefaultTarget );
    FormData fdlDefaultTarget = new FormData();
    fdlDefaultTarget.left = new FormAttachment( 0, 0 );
    fdlDefaultTarget.right = new FormAttachment( middle, 0 );
    fdlDefaultTarget.bottom = new FormAttachment( wOK, -margin * 2 );
    wlDefaultTarget.setLayoutData( fdlDefaultTarget );
    wDefaultTarget = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDefaultTarget );
    FormData fdDefaultTarget = new FormData();
    fdDefaultTarget.left = new FormAttachment( middle, margin );
    fdDefaultTarget.right = new FormAttachment( 100, 0 );
    fdDefaultTarget.bottom = new FormAttachment( wOK, -margin * 2 );
    wDefaultTarget.setLayoutData( fdDefaultTarget );
    wDefaultTarget.setItems( nextStepNames );
    wDefaultTarget.addModifyListener( lsMod );

    FormData fdValues = new FormData();
    fdValues.left = new FormAttachment( middle, margin );
    fdValues.top = new FormAttachment( wGroupingSymbol, margin );
    fdValues.right = new FormAttachment( 100, 0 );
    fdValues.bottom = new FormAttachment( wDefaultTarget, -margin );
    wValues.setLayoutData( fdValues );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

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
    return stepname;
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
        item.setText( 2, target.caseTargetStep == null ? "" : target.caseTargetStep.getName() ); // The target step name
      }
    }
    wValues.removeEmptyRows();
    wValues.setRowNums();
    wValues.optWidth( true );

    wDefaultTarget.setText( input.getDefaultTargetStep() == null ? "" : input.getDefaultTargetStep().getName() );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
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
      target.caseTargetStep = transMeta.findStep( item.getText( 2 ) );
      input.getCaseTargets().add( target );
    }

    input.setDefaultTargetStep( transMeta.findStep( wDefaultTarget.getText() ) );

    stepname = wStepname.getText(); // return value

    dispose();
  }
}
