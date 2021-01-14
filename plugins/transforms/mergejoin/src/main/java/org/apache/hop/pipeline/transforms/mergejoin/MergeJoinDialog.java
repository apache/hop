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

package org.apache.hop.pipeline.transforms.mergejoin;

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
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
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

import java.util.List;

public class MergeJoinDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MergeJoinMeta.class; // For Translator

  public static final String STRING_SORT_WARNING_PARAMETER = "MergeJoinSortWarning";

  private CCombo wTransform1;

  private CCombo wTransform2;

  private CCombo wType;

  private TableView wKeys1;

  private TableView wKeys2;

  private final MergeJoinMeta input;

  public MergeJoinDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (MergeJoinMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MergeJoinDialog.TransformName.Label" ) );
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
    String[] previousTransforms = pipelineMeta.getPrevTransformNames( transformName );

    // First transform
    Label wlTransform1 = new Label(shell, SWT.RIGHT);
    wlTransform1.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Transform1.Label" ) );
    props.setLook(wlTransform1);
    FormData fdlTransform1 = new FormData();
    fdlTransform1.left = new FormAttachment( 0, 0 );
    fdlTransform1.right = new FormAttachment( middle, -margin );
    fdlTransform1.top = new FormAttachment( wTransformName, margin );
    wlTransform1.setLayoutData(fdlTransform1);
    wTransform1 = new CCombo( shell, SWT.BORDER );
    props.setLook( wTransform1 );

    if ( previousTransforms != null ) {
      wTransform1.setItems( previousTransforms );
    }

    wTransform1.addModifyListener( lsMod );
    FormData fdTransform1 = new FormData();
    fdTransform1.left = new FormAttachment( middle, 0 );
    fdTransform1.top = new FormAttachment( wTransformName, margin );
    fdTransform1.right = new FormAttachment( 100, 0 );
    wTransform1.setLayoutData(fdTransform1);

    // Second transform
    Label wlTransform2 = new Label(shell, SWT.RIGHT);
    wlTransform2.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Transform2.Label" ) );
    props.setLook(wlTransform2);
    FormData fdlTransform2 = new FormData();
    fdlTransform2.left = new FormAttachment( 0, 0 );
    fdlTransform2.right = new FormAttachment( middle, -margin );
    fdlTransform2.top = new FormAttachment( wTransform1, margin );
    wlTransform2.setLayoutData(fdlTransform2);
    wTransform2 = new CCombo( shell, SWT.BORDER );
    props.setLook( wTransform2 );

    if ( previousTransforms != null ) {
      wTransform2.setItems( previousTransforms );
    }

    wTransform2.addModifyListener( lsMod );
    FormData fdTransform2 = new FormData();
    fdTransform2.top = new FormAttachment( wTransform1, margin );
    fdTransform2.left = new FormAttachment( middle, 0 );
    fdTransform2.right = new FormAttachment( 100, 0 );
    wTransform2.setLayoutData(fdTransform2);

    // Join type
    Label wlType = new Label(shell, SWT.RIGHT);
    wlType.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Type.Label" ) );
    props.setLook(wlType);
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment( 0, 0 );
    fdlType.right = new FormAttachment( middle, -margin );
    fdlType.top = new FormAttachment( wTransform2, margin );
    wlType.setLayoutData(fdlType);
    wType = new CCombo( shell, SWT.BORDER );
    props.setLook( wType );

    wType.setItems( MergeJoinMeta.joinTypes );

    wType.addModifyListener( lsMod );
    FormData fdType = new FormData();
    fdType.top = new FormAttachment( wTransform2, margin );
    fdType.left = new FormAttachment( middle, 0 );
    fdType.right = new FormAttachment( 100, 0 );
    wType.setLayoutData(fdType);

    // Some buttons at the bottom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e->ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    Button wbKeys1 = new Button(shell, SWT.PUSH);
    wbKeys1.setText( BaseMessages.getString( PKG, "MergeJoinDialog.KeyFields1.Button" ) );
    FormData fdbKeys1 = new FormData();
    fdbKeys1.bottom = new FormAttachment( wOk, -2*margin );
    fdbKeys1.left = new FormAttachment( 0, 0 );
    fdbKeys1.right = new FormAttachment( 50, -margin );
    wbKeys1.setLayoutData(fdbKeys1);
    wbKeys1.addSelectionListener(new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        getKeys1();
      }
    } );

    Button wbKeys2 = new Button(shell, SWT.PUSH);
    wbKeys2.setText( BaseMessages.getString( PKG, "MergeJoinDialog.KeyFields2.Button" ) );
    FormData fdbKeys2 = new FormData();
    fdbKeys2.bottom = new FormAttachment( wOk, -2*margin );
    fdbKeys2.left = new FormAttachment( 50, 0 );
    fdbKeys2.right = new FormAttachment( 100, 0 );
    wbKeys2.setLayoutData(fdbKeys2);
    wbKeys2.addSelectionListener(new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        getKeys2();
      }
    } );

    // Now the lists of keys between the label and the lower buttons
    //

    // THE KEYS TO MATCH for first transform...
    Label wlKeys1 = new Label(shell, SWT.NONE);
    wlKeys1.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Keys1.Label" ) );
    props.setLook(wlKeys1);
    FormData fdlKeys1 = new FormData();
    fdlKeys1.left = new FormAttachment( 0, 0 );
    fdlKeys1.top = new FormAttachment( wType, margin );
    wlKeys1.setLayoutData(fdlKeys1);

    int nrKeyRows1 = ( input.getKeyFields1() != null ? input.getKeyFields1().length : 1 );

    ColumnInfo[] ciKeys1 =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "MergeJoinDialog.ColumnInfo.KeyField1" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false ), };

    wKeys1 = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      ciKeys1, nrKeyRows1, lsMod, props );
    FormData fdKeys1 = new FormData();
    fdKeys1.top = new FormAttachment(wlKeys1, margin );
    fdKeys1.left = new FormAttachment( 0, 0 );
    fdKeys1.bottom = new FormAttachment(wbKeys1, -2*margin );
    fdKeys1.right = new FormAttachment( 50, -margin );
    wKeys1.setLayoutData(fdKeys1);


    // THE KEYS TO MATCH for second transform
    Label wlKeys2 = new Label(shell, SWT.NONE);
    wlKeys2.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Keys2.Label" ) );
    props.setLook(wlKeys2);
    FormData fdlKeys2 = new FormData();
    fdlKeys2.left = new FormAttachment( 50, 0 );
    fdlKeys2.top = new FormAttachment( wType, margin );
    wlKeys2.setLayoutData(fdlKeys2);

    int nrKeyRows2 = ( input.getKeyFields2() != null ? input.getKeyFields2().length : 1 );

    ColumnInfo[] ciKeys2 =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "MergeJoinDialog.ColumnInfo.KeyField2" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false ), };

    wKeys2 = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      ciKeys2, nrKeyRows2, lsMod, props );
    FormData fdKeys2 = new FormData();
    fdKeys2.top = new FormAttachment(wlKeys2, margin );
    fdKeys2.left = new FormAttachment( 50, 0 );
    fdKeys2.bottom = new FormAttachment(wbKeys2, -2*margin );
    fdKeys2.right = new FormAttachment( 100, 0 );
    wKeys2.setLayoutData(fdKeys2);




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
    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();

    wTransform1.setText( Const.NVL( infoStreams.get( 0 ).getTransformName(), "" ) );
    wTransform2.setText( Const.NVL( infoStreams.get( 1 ).getTransformName(), "" ) );
    String joinType = input.getJoinType();
    if ( joinType != null && joinType.length() > 0 ) {
      wType.setText( joinType );
    } else {
      wType.setText( MergeJoinMeta.joinTypes[ 0 ] );
    }

    for ( int i = 0; i < input.getKeyFields1().length; i++ ) {
      TableItem item = wKeys1.table.getItem( i );
      if ( input.getKeyFields1()[ i ] != null ) {
        item.setText( 1, input.getKeyFields1()[ i ] );
      }
    }
    for ( int i = 0; i < input.getKeyFields2().length; i++ ) {
      TableItem item = wKeys2.table.getItem( i );
      if ( input.getKeyFields2()[ i ] != null ) {
        item.setText( 1, input.getKeyFields2()[ i ] );
      }
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void getMeta( MergeJoinMeta meta ) {
    List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();

    infoStreams.get( 0 ).setTransformMeta( pipelineMeta.findTransform( wTransform1.getText() ) );
    infoStreams.get( 1 ).setTransformMeta( pipelineMeta.findTransform( wTransform2.getText() ) );
    meta.setJoinType( wType.getText() );

    int nrKeys1 = wKeys1.nrNonEmpty();
    int nrKeys2 = wKeys2.nrNonEmpty();

    meta.allocate( nrKeys1, nrKeys2 );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrKeys1; i++ ) {
      TableItem item = wKeys1.getNonEmpty( i );
      meta.getKeyFields1()[ i ] = item.getText( 1 );
    }

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrKeys2; i++ ) {
      TableItem item = wKeys2.getNonEmpty( i );
      meta.getKeyFields2()[ i ] = item.getText( 1 );
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    getMeta( input );

    // Show a warning (optional)
    //
    if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.DialogTitle" ),
          BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.DialogMessage", Const.CR ) + Const.CR,
          SWT.ICON_WARNING,
          new String[] { BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.Option1" ) },
          BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.Option2" ), "N".equalsIgnoreCase(
          props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) );
      md.open();
      props.setCustomParameter( STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
    }

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void getKeys1() {
    MergeJoinMeta joinMeta = new MergeJoinMeta();
    getMeta( joinMeta );

    try {
      List<IStream> infoStreams = joinMeta.getTransformIOMeta().getInfoStreams();

      TransformMeta transformMeta = infoStreams.get( 0 ).getTransformMeta();
      if ( transformMeta != null ) {
        IRowMeta prev = pipelineMeta.getTransformFields( variables, transformMeta );
        if ( prev != null ) {
          BaseTransformDialog.getFieldsFromPrevious( prev, wKeys1, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
        }
      }
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MergeJoinDialog.ErrorGettingFields.DialogTitle" ), BaseMessages
        .getString( PKG, "MergeJoinDialog.ErrorGettingFields.DialogMessage" ), e );
    }
  }

  private void getKeys2() {
    MergeJoinMeta joinMeta = new MergeJoinMeta();
    getMeta( joinMeta );

    try {
      List<IStream> infoStreams = joinMeta.getTransformIOMeta().getInfoStreams();

      TransformMeta transformMeta = infoStreams.get( 1 ).getTransformMeta();
      if ( transformMeta != null ) {
        IRowMeta prev = pipelineMeta.getTransformFields( variables, transformMeta );
        if ( prev != null ) {
          BaseTransformDialog.getFieldsFromPrevious( prev, wKeys2, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
        }
      }
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MergeJoinDialog.ErrorGettingFields.DialogTitle" ), BaseMessages
        .getString( PKG, "MergeJoinDialog.ErrorGettingFields.DialogMessage" ), e );
    }
  }

}
