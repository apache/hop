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

package org.apache.hop.pipeline.transforms.numberrange;

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
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class NumberRangeDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = NumberRangeMeta.class; // For Translator

  private final NumberRangeMeta input;

  private CCombo inputFieldControl;
  private Text outputFieldControl;
  private Text fallBackValueControl;
  private TableView rulesControl;

  public NumberRangeDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (NumberRangeMeta) in;
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

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "NumberRange.TypeLongDesc" ) );

    // Some buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( "OK" );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( "Cancel" );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, props.getMargin(), null );

    // Create controls
    wTransformName = createLine( lsMod, BaseMessages.getString( PKG, "NumberRange.TransformName" ), null );
    inputFieldControl = createLineCombo( lsMod, BaseMessages.getString( PKG, "NumberRange.InputField" ), wTransformName );
    outputFieldControl = createLine( lsMod, BaseMessages.getString( PKG, "NumberRange.OutputField" ), inputFieldControl );

    inputFieldControl.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        loadComboOptions();
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    fallBackValueControl = createLine( lsMod, BaseMessages.getString( PKG, "NumberRange.DefaultValue" ), outputFieldControl );

    createRulesTable( lsMod );

    // Add listeners

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    inputFieldControl.addSelectionListener( lsDef );

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
   * Creates the table of rules
   */
  private void createRulesTable( ModifyListener lsMod ) {
    Label rulesLable = new Label( shell, SWT.NONE );
    rulesLable.setText( BaseMessages.getString( PKG, "NumberRange.Ranges" ) );
    props.setLook( rulesLable );
    FormData lableFormData = new FormData();
    lableFormData.left = new FormAttachment( 0, 0 );
    lableFormData.right = new FormAttachment( props.getMiddlePct(), -props.getMargin() );
    lableFormData.top = new FormAttachment( fallBackValueControl, props.getMargin() );
    rulesLable.setLayoutData( lableFormData );

    final int FieldsRows = input.getRules().size();

    ColumnInfo[] colinf = new ColumnInfo[ 3 ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "NumberRange.LowerBound" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "NumberRange.UpperBound" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 2 ] =
      new ColumnInfo( BaseMessages.getString( PKG, "NumberRange.Value" ), ColumnInfo.COLUMN_TYPE_TEXT, false );

    rulesControl = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( rulesLable, props.getMargin() );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2 * props.getMargin() );
    rulesControl.setLayoutData( fdFields );
  }

  private Text createLine( ModifyListener lsMod, String lableText, Control prevControl ) {
    // Value line
    Label lable = new Label( shell, SWT.RIGHT );
    lable.setText( lableText );
    props.setLook( lable );
    FormData lableFormData = new FormData();
    lableFormData.left = new FormAttachment( 0, 0 );
    lableFormData.right = new FormAttachment( props.getMiddlePct(), -props.getMargin() );
    // In case it is the first control
    if ( prevControl != null ) {
      lableFormData.top = new FormAttachment( prevControl, props.getMargin() );
    } else {
      lableFormData.top = new FormAttachment( 0, props.getMargin() );
    }
    lable.setLayoutData( lableFormData );

    Text control = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( control );
    control.addModifyListener( lsMod );
    FormData widgetFormData = new FormData();
    widgetFormData.left = new FormAttachment( props.getMiddlePct(), 0 );
    // In case it is the first control
    if ( prevControl != null ) {
      widgetFormData.top = new FormAttachment( prevControl, props.getMargin() );
    } else {
      widgetFormData.top = new FormAttachment( 0, props.getMargin() );
    }
    widgetFormData.right = new FormAttachment( 100, 0 );
    control.setLayoutData( widgetFormData );

    return control;
  }

  private CCombo createLineCombo( ModifyListener lsMod, String lableText, Control prevControl ) {
    // Value line
    Label lable = new Label( shell, SWT.RIGHT );
    lable.setText( lableText );
    props.setLook( lable );
    FormData lableFormData = new FormData();
    lableFormData.left = new FormAttachment( 0, 0 );
    lableFormData.right = new FormAttachment( props.getMiddlePct(), -props.getMargin() );
    // In case it is the first control
    if ( prevControl != null ) {
      lableFormData.top = new FormAttachment( prevControl, props.getMargin() );
    } else {
      lableFormData.top = new FormAttachment( 0, props.getMargin() );
    }
    lable.setLayoutData( lableFormData );

    CCombo control = new CCombo( shell, SWT.BORDER );
    props.setLook( control );
    control.addModifyListener( lsMod );
    FormData widgetFormData = new FormData();
    widgetFormData.left = new FormAttachment( props.getMiddlePct(), 0 );
    // In case it is the first control
    if ( prevControl != null ) {
      widgetFormData.top = new FormAttachment( prevControl, props.getMargin() );
    } else {
      widgetFormData.top = new FormAttachment( 0, props.getMargin() );
    }
    widgetFormData.right = new FormAttachment( 100, 0 );
    control.setLayoutData( widgetFormData );

    return control;
  }

  // Read data from input (TextFileInputInfo)
  public void getData() {
    // Get fields

    wTransformName.setText( transformName );

    String inputField = input.getInputField();
    if ( inputField != null ) {
      inputFieldControl.setText( inputField );
    }

    String outputField = input.getOutputField();
    if ( outputField != null ) {
      outputFieldControl.setText( outputField );
    }

    String fallBackValue = input.getFallBackValue();
    if ( fallBackValue != null ) {
      fallBackValueControl.setText( fallBackValue );
    }

    for ( int i = 0; i < input.getRules().size(); i++ ) {
      NumberRangeRule rule = input.getRules().get( i );
      TableItem item = rulesControl.table.getItem( i );

      // Empty value is equal to minimal possible value
      if ( rule.getLowerBound() > -Double.MAX_VALUE ) {
        String lowerBoundStr = String.valueOf( rule.getLowerBound() );
        item.setText( 1, lowerBoundStr );
      }

      // Empty value is equal to maximal possible value
      if ( rule.getUpperBound() < Double.MAX_VALUE ) {
        String upperBoundStr = String.valueOf( rule.getUpperBound() );
        item.setText( 2, upperBoundStr );
      }

      item.setText( 3, rule.getValue() );
    }
    rulesControl.setRowNums();
    rulesControl.optWidth( true );
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

    String inputField = inputFieldControl.getText();
    input.setInputField( inputField );

    input.emptyRules();

    String fallBackValue = fallBackValueControl.getText();
    input.setFallBackValue( fallBackValue );

    input.setOutputField( outputFieldControl.getText() );

    int count = rulesControl.nrNonEmpty();
    for ( int i = 0; i < count; i++ ) {
      TableItem item = rulesControl.getNonEmpty( i );
      String lowerBoundStr =
        Utils.isEmpty( item.getText( 1 ) ) ? String.valueOf( -Double.MAX_VALUE ) : item.getText( 1 );
      String upperBoundStr =
        Utils.isEmpty( item.getText( 2 ) ) ? String.valueOf( Double.MAX_VALUE ) : item.getText( 2 );
      String value = item.getText( 3 );

      try {
        double lowerBound = Double.parseDouble( lowerBoundStr );
        double upperBound = Double.parseDouble( upperBoundStr );

        input.addRule( lowerBound, upperBound, value );
      } catch ( NumberFormatException e ) {
        throw new IllegalArgumentException( "Bounds of this rule are not numeric: lowerBound="
          + lowerBoundStr + ", upperBound=" + upperBoundStr + ", value=" + value, e );
      }
    }

    dispose();
  }

  private void loadComboOptions() {
    try {
      String fieldvalue = null;
      if ( inputFieldControl.getText() != null ) {
        fieldvalue = inputFieldControl.getText();
      }
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        inputFieldControl.setItems( r.getFieldNames() );
      }
      if ( fieldvalue != null ) {
        inputFieldControl.setText( fieldvalue );
      }

    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "NumberRange.TypeLongDesc" ), "Can't get fields", ke );
    }
  }

}
