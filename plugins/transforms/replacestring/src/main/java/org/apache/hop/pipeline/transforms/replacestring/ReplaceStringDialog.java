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

package org.apache.hop.pipeline.transforms.replacestring;


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

/**
 * Search and replace in string.
 *
 * @author Samatar Hassan
 * @since 28 September 2007
 */
public class ReplaceStringDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ReplaceStringMeta.class; // For Translator

  private TableView wFields;

  private final ReplaceStringMeta input;

  private final Map<String, Integer> inputFields;

  private ColumnInfo[] ciKey;

  public ReplaceStringDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (ReplaceStringMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "ReplaceStringDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ReplaceStringDialog.TransformName.Label" ) );
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

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "ReplaceStringDialog.GetFields.Button" ) );
    wGet.addListener( SWT.Selection, e -> get() );
    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );

    Label wlKey = new Label(shell, SWT.NONE);
    wlKey.setText( BaseMessages.getString( PKG, "ReplaceStringDialog.Fields.Label" ) );
    props.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.top = new FormAttachment( wTransformName, 2 * margin );
    wlKey.setLayoutData(fdlKey);

    int nrFieldCols = 10;
    int nrFieldRows = ( input.getFieldInStream() != null ? input.getFieldInStream().length : 1 );

    ciKey = new ColumnInfo[ nrFieldCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.InStreamField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciKey[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.OutStreamField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    ciKey[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.useRegEx" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ReplaceStringMeta.useRegExDesc );
    ciKey[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.Replace" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    ciKey[ 4 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.By" ), ColumnInfo.COLUMN_TYPE_TEXT, false );
    ciKey[ 5 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.SetEmptyString" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ), BaseMessages.getString( PKG, "System.Combo.No" ) } );

    ciKey[ 6 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.FieldReplaceBy" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );

    ciKey[ 7 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.WholeWord" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ReplaceStringMeta.wholeWordDesc );
    ciKey[ 8 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.CaseSensitive" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ReplaceStringMeta.caseSensitiveDesc );
    ciKey[ 9 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.IsUnicode" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ReplaceStringMeta.isUnicodeDesc );

    ciKey[ 1 ].setToolTip( BaseMessages.getString( PKG, "ReplaceStringDialog.ColumnInfo.OutStreamField.Tooltip" ) );
    ciKey[ 1 ].setUsingVariables( true );
    ciKey[ 3 ].setUsingVariables( true );
    ciKey[ 4 ].setUsingVariables( true );

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
          logError( BaseMessages.getString( PKG, "ReplaceString.Error.CanNotGetFields" ) );
        }
      }
    };
    new Thread( runnable ).start();





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
    ciKey[ 6 ].setComboValues( fieldNames );
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
        item.setText( 3, ReplaceStringMeta.getUseRegExDesc( input.getUseRegEx()[ i ] ) );
        if ( input.getReplaceString()[ i ] != null ) {
          item.setText( 4, input.getReplaceString()[ i ] );
        }
        if ( input.getReplaceByString()[ i ] != null ) {
          item.setText( 5, input.getReplaceByString()[ i ] );
        }
        item
          .setText( 6, input.isSetEmptyString()[ i ]
            ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" ) );

        if ( input.getFieldReplaceByString()[ i ] != null ) {
          item.setText( 7, input.getFieldReplaceByString()[ i ] );
        }

        item.setText( 8, ReplaceStringMeta.getWholeWordDesc( input.getWholeWord()[ i ] ) );
        item.setText( 9, ReplaceStringMeta.getCaseSensitiveDesc( input.getCaseSensitive()[ i ] ) );
        item.setText( 10, ReplaceStringMeta.getIsUnicodeDesc( input.isUnicode()[ i ] ) );
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

  private void getInfo( ReplaceStringMeta inf ) {

    int nrkeys = wFields.nrNonEmpty();

    inf.allocate( nrkeys );
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "ReplaceStringDialog.Log.FoundFields", String.valueOf( nrkeys ) ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrkeys; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      inf.getFieldInStream()[ i ] = item.getText( 1 );
      inf.getFieldOutStream()[ i ] = item.getText( 2 );
      inf.getUseRegEx()[ i ] = ReplaceStringMeta.getUseRegExByDesc( item.getText( 3 ) );
      inf.getReplaceString()[ i ] = item.getText( 4 );
      inf.getReplaceByString()[ i ] = item.getText( 5 );

      inf.isSetEmptyString()[ i ] =
        BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( 6 ) );
      if ( inf.isSetEmptyString()[ i ] ) {
        inf.getReplaceByString()[ i ] = "";
      }
      inf.getFieldReplaceByString()[ i ] = item.getText( 7 );
      if ( !Utils.isEmpty( item.getText( 7 ) ) ) {
        inf.getReplaceByString()[ i ] = "";
      }

      inf.getWholeWord()[ i ] = ReplaceStringMeta.getWholeWordByDesc( item.getText( 8 ) );
      inf.getCaseSensitive()[ i ] = ReplaceStringMeta.getCaseSensitiveByDesc( item.getText( 9 ) );
      inf.isUnicode()[ i ] = ReplaceStringMeta.getIsUnicodeByDesc( item.getText( 10 ) );
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
            tableItem.setText( 3, BaseMessages.getString( PKG, "System.Combo.No" ) );
            tableItem.setText( 6, BaseMessages.getString( PKG, "System.Combo.No" ) );
            tableItem.setText( 8, BaseMessages.getString( PKG, "System.Combo.No" ) );
            tableItem.setText( 9, BaseMessages.getString( PKG, "System.Combo.No" ) );
            tableItem.setText( 10, BaseMessages.getString( PKG, "System.Combo.No" ) );
            return true;
          } else {
            return false;
          }
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "ReplaceStringDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "ReplaceStringDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }
}
