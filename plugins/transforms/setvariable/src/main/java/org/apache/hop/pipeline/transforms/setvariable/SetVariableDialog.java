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

package org.apache.hop.pipeline.transforms.setvariable;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;
import org.apache.hop.core.variables.IVariables;

public class SetVariableDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SetVariableMeta.class; // For Translator

  public static final String STRING_USAGE_WARNING_PARAMETER = "SetVariableUsageWarning";

  private Text wTransformName;

  private Button wFormat;

  private TableView wFields;

  private final SetVariableMeta input;

  private final Map<String, Integer> inputFields;

  private ColumnInfo[] colinf;

  public SetVariableDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (SetVariableMeta) in;
    inputFields = new HashMap<>();
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
    shell.setText( BaseMessages.getString( PKG, "SetVariableDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection,  e -> ok() );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.addListener( SWT.Selection, e -> get() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection,  e -> cancel() );
    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );


    // TransformName line
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText( BaseMessages.getString( PKG, "SetVariableDialog.TransformName.Label" ) );
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

    Label wlFormat = new Label(shell, SWT.RIGHT);
    wlFormat.setText( BaseMessages.getString( PKG, "SetVariableDialog.Format.Label" ) );
    wlFormat.setToolTipText( BaseMessages.getString( PKG, "SetVariableDialog.Format.Tooltip" ) );
    props.setLook(wlFormat);
    FormData fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment( 0, 0 );
    fdlFormat.right = new FormAttachment( middle, -margin );
    fdlFormat.top = new FormAttachment( wTransformName, margin );
    wlFormat.setLayoutData(fdlFormat);
    wFormat = new Button( shell, SWT.CHECK );
    wFormat.setToolTipText( BaseMessages.getString( PKG, "SetVariableDialog.Format.Tooltip" ) );
    props.setLook( wFormat );
    FormData fdFormat = new FormData();
    fdFormat.left = new FormAttachment( middle, 0 );
    fdFormat.top = new FormAttachment( wlFormat, 0, SWT.CENTER );
    wFormat.setLayoutData(fdFormat);
    wFormat.addSelectionListener( new ComponentSelectionListener( input ) );

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "SetVariableDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wFormat, margin );
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = input.getFieldName().length;
    colinf = new ColumnInfo[ 4 ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.FieldName" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.VariableName" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.VariableType" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, SetVariableMeta.getVariableTypeDescriptions(), false );
    colinf[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.DefaultValue" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 3 ].setUsingVariables( true );
    colinf[ 3 ].setToolTip( BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.DefaultValue.Tooltip" ) );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2*margin );
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

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wTransformName.setText( transformName );

    for ( int i = 0; i < input.getFieldName().length; i++ ) {
      TableItem item = wFields.table.getItem( i );
      String src = input.getFieldName()[ i ];
      String tgt = input.getVariableName()[ i ];
      String typ = SetVariableMeta.getVariableTypeDescription( input.getVariableType()[ i ] );
      String tvv = input.getDefaultValue()[ i ];

      if ( src != null ) {
        item.setText( 1, src );
      }
      if ( tgt != null ) {
        item.setText( 2, tgt );
      }
      if ( typ != null ) {
        item.setText( 3, typ );
      }
      if ( tvv != null ) {
        item.setText( 4, tvv );
      }
    }

    wFormat.setSelection( input.isUsingFormatting() );

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

    int count = wFields.nrNonEmpty();
    input.allocate( count );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < count; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getFieldName()[ i ] = item.getText( 1 );
      input.getVariableName()[ i ] = item.getText( 2 );
      input.getVariableType()[ i ] = SetVariableMeta.getVariableType( item.getText( 3 ) );
      input.getDefaultValue()[ i ] = item.getText( 4 );
    }

    input.setUsingFormatting( wFormat.getSelection() );

    // Show a warning (optional)
    //
    if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_USAGE_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "SetVariableDialog.UsageWarning.DialogTitle" ),
          BaseMessages.getString( PKG, "SetVariableDialog.UsageWarning.DialogMessage", Const.CR ) + Const.CR,
          SWT.ICON_WARNING,
          new String[] { BaseMessages.getString( PKG, "SetVariableDialog.UsageWarning.Option1" ) },
          BaseMessages.getString( PKG, "SetVariableDialog.UsageWarning.Option2" ),
          "N".equalsIgnoreCase( props.getCustomParameter( STRING_USAGE_WARNING_PARAMETER, "Y" ) ) );
      md.open();
      props.setCustomParameter( STRING_USAGE_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious(
          r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, ( tableItem, v ) -> {
            tableItem.setText( 2, v.getName().toUpperCase() );
            tableItem.setText( 3, SetVariableMeta
              .getVariableTypeDescription( SetVariableMeta.VARIABLE_TYPE_ROOT_WORKFLOW ) );
            return true;
          } );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "SetVariableDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "Set.FailedToGetFields.DialogMessage" ), ke );
    }
  }
}
