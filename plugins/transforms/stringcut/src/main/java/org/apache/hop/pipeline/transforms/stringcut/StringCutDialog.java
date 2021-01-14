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

package org.apache.hop.pipeline.transforms.stringcut;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;
import org.apache.hop.core.variables.IVariables;

public class StringCutDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = StringCutMeta.class; // For Translator

  private TableView wFields;

  private final StringCutMeta input;

  private final Map<String, Integer> inputFields;

  private ColumnInfo[] ciKey;

  public StringCutDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (StringCutMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "StringCutDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "StringCutDialog.GetFields.Button" ) );
    wGet.addListener( SWT.Selection, e -> get() );
    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "StringCutDialog.TransformName.Label" ) );
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

    Label wlKey = new Label(shell, SWT.NONE);
    wlKey.setText( BaseMessages.getString( PKG, "StringCutDialog.Fields.Label" ) );
    props.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.top = new FormAttachment( wTransformName, 2 * margin );
    wlKey.setLayoutData(fdlKey);

    int nrFieldCols = 4;
    int nrFieldRows = ( input.getFieldInStream() != null ? input.getFieldInStream().length : 1 );

    ciKey = new ColumnInfo[ nrFieldCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StringCutDialog.ColumnInfo.InStreamField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciKey[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StringCutDialog.ColumnInfo.OutStreamField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    ciKey[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StringCutDialog.ColumnInfo.CutFrom" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    ciKey[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StringCutDialog.ColumnInfo.CutTo" ), ColumnInfo.COLUMN_TYPE_TEXT, false );

    ciKey[ 2 ].setUsingVariables( true );
    ciKey[ 1 ].setToolTip( BaseMessages.getString( PKG, "StringCutDialog.ColumnInfo.OutStreamField.Tooltip" ) );
    ciKey[ 3 ].setUsingVariables( true );

    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
        nrFieldRows, lsMod, props );

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment( 0, 0 );
    fdKey.top = new FormAttachment(wlKey, margin );
    fdKey.right = new FormAttachment( 100, -margin );
    fdKey.bottom = new FormAttachment( wOk, -2*margin );
    wFields.setLayoutData(fdKey);

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
          logError( "It was not possible to get the fields from the previous transform(s)." );
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
    ciKey[ 0 ].setComboValues( fieldNames );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getFieldInStream() != null ) {
      for ( int i = 0; i < input.getFieldInStream().length; i++ ) {
        TableItem item = wFields.table.getItem( i );
        if ( input.getFieldInStream()[ i ] != null ) {
          item.setText( 1, input.getFieldInStream()[ i ] );
        }
        if ( input.getFieldOutStream()[ i ] != null ) {
          item.setText( 2, input.getFieldOutStream()[ i ] );
        }
        if ( input.getCutFrom()[ i ] != null ) {
          item.setText( 3, input.getCutFrom()[ i ] );
        }
        if ( input.getCutTo()[ i ] != null ) {
          item.setText( 4, input.getCutTo()[ i ] );
        }
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

  private void getInfo( StringCutMeta inf ) {
    int nrkeys = wFields.nrNonEmpty();

    inf.allocate( nrkeys );
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StringCutDialog.Log.FoundFields", String.valueOf( nrkeys ) ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrkeys; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      inf.getFieldInStream()[ i ] = item.getText( 1 );
      inf.getFieldOutStream()[ i ] = item.getText( 2 );
      inf.getCutFrom()[ i ] = item.getText( 3 );
      inf.getCutTo()[ i ] = item.getText( 4 );
    }

    transformName = wTransformName.getText(); // return value
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    // Get the information for the dialog into the input structure.
    getInfo( input );

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = ( tableItem, v ) -> {
          if ( v.getType() == IValueMeta.TYPE_STRING ) {
            // Only process strings
            return true;
          } else {
            return false;
          }
        };

        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, listener );

      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "StringCutDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "StringCutDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }
}
