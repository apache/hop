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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.ArrayList;
import java.util.List;

public class BlockUntilTransformsFinishDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BlockUntilTransformsFinishMeta.class; // For Translator

  private String[] previousTransforms;
  private final BlockUntilTransformsFinishMeta input;

  private TableView wFields;

  public BlockUntilTransformsFinishDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (BlockUntilTransformsFinishMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BlockUntilTransformsFinishDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "BlockUntilTransformsFinishDialog.TransformName.Label" ) );
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

    // Get the previous transforms...
    setTransformNames();

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "BlockUntilTransformsFinishDialog.getTransforms.Label" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );

    // Table with fields
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "BlockUntilTransformsFinishDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wTransformName, margin );
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 2;
    final int FieldsRows = input.getTransformName().length;

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "BlockUntilTransformsFinishDialog.Fieldname.transform" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, previousTransforms, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "BlockUntilTransformsFinishDialog.Fieldname.CopyNr" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[ 1 ].setUsingVariables( true );
    wFields =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2 * margin );
    wFields.setLayoutData(fdFields);

    // Add listeners
    lsCancel = e -> cancel();
    lsGet = e -> get();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );
    wGet.addListener( SWT.Selection, lsGet );

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

  private void setTransformNames() {
    previousTransforms = pipelineMeta.getTransformNames();
    String[] nextTransforms = pipelineMeta.getNextTransformNames( transformMeta );

    List<String> entries = new ArrayList<>();
    for (String previousTransform : previousTransforms) {
      if (!previousTransform.equals(transformName)) {
        if (nextTransforms != null) {
          for (String nextTransform : nextTransforms) {
            if (!nextTransform.equals(previousTransform)) {
              entries.add(previousTransform);
            }
          }
        }
      }
    }
    previousTransforms = entries.toArray( new String[ entries.size() ] );
  }

  private void get() {
    wFields.removeAll();
    Table table = wFields.table;

    for ( int i = 0; i < previousTransforms.length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      ti.setText( 1, previousTransforms[ i ] );
      ti.setText( 2, "0" );
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    Table table = wFields.table;
    if ( input.getTransformName().length > 0 ) {
      table.removeAll();
    }
    for ( int i = 0; i < input.getTransformName().length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      if ( input.getTransformName()[ i ] != null ) {
        ti.setText( 1, input.getTransformName()[ i ] );
        ti.setText( 2, "" + Const.toInt( input.getTransformCopyNr()[ i ], 0 ) );
      }
    }

    wFields.removeEmptyRows();
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

    int nrTransforms = wFields.nrNonEmpty();
    input.allocate( nrTransforms );
    for ( int i = 0; i < nrTransforms; i++ ) {
      TableItem ti = wFields.getNonEmpty( i );
      TransformMeta tm = pipelineMeta.findTransform( ti.getText( 1 ) );
      if ( tm != null ) {
        //CHECKSTYLE:Indentation:OFF
        input.getTransformName()[ i ] = tm.getName();
        input.getTransformCopyNr()[ i ] = String.valueOf( Const.toInt( ti.getText( 2 ), 0 ) );
      }

    }
    dispose();
  }
}
