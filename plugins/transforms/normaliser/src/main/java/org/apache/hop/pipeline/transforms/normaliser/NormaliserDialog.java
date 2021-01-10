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

package org.apache.hop.pipeline.transforms.normaliser;

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
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class NormaliserDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = NormaliserMeta.class; // For Translator

  private static final int NAME_INDEX = 1;

  private static final int VALUE_INDEX = 2;

  private static final int NORM_INDEX = 3;

  private Text wTypefield;

  private TableView wFields;

  private final NormaliserMeta input;

  private ColumnInfo[] colinf;

  private final Map<String, Integer> inputFields;

  public NormaliserDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (NormaliserMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "NormaliserDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "NormaliserDialog.GetFields.Button" ) );
    wGet.addListener( SWT.Selection, e -> get() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "NormaliserDialog.TransformName.Label" ) );
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

    // Typefield line
    Label wlTypefield = new Label(shell, SWT.RIGHT);
    wlTypefield.setText( BaseMessages.getString( PKG, "NormaliserDialog.TypeField.Label" ) );
    props.setLook(wlTypefield);
    FormData fdlTypefield = new FormData();
    fdlTypefield.left = new FormAttachment( 0, 0 );
    fdlTypefield.right = new FormAttachment( middle, -margin );
    fdlTypefield.top = new FormAttachment( wTransformName, margin );
    wlTypefield.setLayoutData(fdlTypefield);
    wTypefield = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTypefield.setText( "" );
    props.setLook( wTypefield );
    wTypefield.addModifyListener( lsMod );
    FormData fdTypefield = new FormData();
    fdTypefield.left = new FormAttachment( middle, 0 );
    fdTypefield.top = new FormAttachment( wTransformName, margin );
    fdTypefield.right = new FormAttachment( 100, 0 );
    wTypefield.setLayoutData(fdTypefield);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "NormaliserDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wTypefield, margin );
    wlFields.setLayoutData(fdlFields);

    final int fieldsCols = 3;
    final int fieldsRows = input.getNormaliserFields().length;

    colinf = new ColumnInfo[ fieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "NormaliserDialog.ColumnInfo.Fieldname" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "NormaliserDialog.ColumnInfo.Type" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "NormaliserDialog.ColumnInfo.NewField" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldsRows, lsMod, props );

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

  public void getData() {
    if ( input.getTypeField() != null ) {
      wTypefield.setText( input.getTypeField() );
    }

    for ( int i = 0; i < input.getNormaliserFields().length; i++ ) {
      TableItem item = wFields.table.getItem( i );
      if ( input.getNormaliserFields()[ i ].getName() != null ) {
        item.setText( NAME_INDEX, input.getNormaliserFields()[ i ].getName() );
      }
      if ( input.getNormaliserFields()[ i ].getValue() != null ) {
        item.setText( VALUE_INDEX, input.getNormaliserFields()[ i ].getValue() );
      }
      if ( input.getNormaliserFields()[ i ].getNorm() != null ) {
        item.setText( NORM_INDEX, input.getNormaliserFields()[ i ].getNorm() );
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

    input.setTypeField( wTypefield.getText() );

    int i;
    // Table table = wFields.table;

    int nrFields = wFields.nrNonEmpty();
    input.allocate( nrFields );

    //CHECKSTYLE:Indentation:OFF
    for ( i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getNormaliserFields()[ i ].setName( item.getText( NAME_INDEX ) );
      input.getNormaliserFields()[ i ].setValue( item.getText( VALUE_INDEX ) );
      input.getNormaliserFields()[ i ].setNorm( item.getText( NORM_INDEX ) );
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "NormaliserDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "NormaliserDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }
}
