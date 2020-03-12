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

package org.apache.hop.ui.trans.steps.mergejoin;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.trans.steps.mergejoin.MergeJoinMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
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

import java.util.List;

public class MergeJoinDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = MergeJoinMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String STRING_SORT_WARNING_PARAMETER = "MergeJoinSortWarning";

  private Label wlStep1;
  private CCombo wStep1;
  private FormData fdlStep1, fdStep1;

  private Label wlStep2;
  private CCombo wStep2;
  private FormData fdlStep2, fdStep2;

  private Label wlType;
  private CCombo wType;
  private FormData fdlType, fdType;

  private Label wlKeys1;
  private TableView wKeys1;
  private Button wbKeys1;
  private FormData fdlKeys1, fdKeys1, fdbKeys1;

  private Label wlKeys2;
  private TableView wKeys2;
  private Button wbKeys2;
  private FormData fdlKeys2, fdKeys2, fdbKeys2;

  private MergeJoinMeta input;

  public MergeJoinDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (MergeJoinMeta) in;
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
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Get the previous steps...
    String[] previousSteps = transMeta.getPrevStepNames( stepname );

    // First step
    wlStep1 = new Label( shell, SWT.RIGHT );
    wlStep1.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Step1.Label" ) );
    props.setLook( wlStep1 );
    fdlStep1 = new FormData();
    fdlStep1.left = new FormAttachment( 0, 0 );
    fdlStep1.right = new FormAttachment( middle, -margin );
    fdlStep1.top = new FormAttachment( wStepname, margin );
    wlStep1.setLayoutData( fdlStep1 );
    wStep1 = new CCombo( shell, SWT.BORDER );
    props.setLook( wStep1 );

    if ( previousSteps != null ) {
      wStep1.setItems( previousSteps );
    }

    wStep1.addModifyListener( lsMod );
    fdStep1 = new FormData();
    fdStep1.left = new FormAttachment( middle, 0 );
    fdStep1.top = new FormAttachment( wStepname, margin );
    fdStep1.right = new FormAttachment( 100, 0 );
    wStep1.setLayoutData( fdStep1 );

    // Second step
    wlStep2 = new Label( shell, SWT.RIGHT );
    wlStep2.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Step2.Label" ) );
    props.setLook( wlStep2 );
    fdlStep2 = new FormData();
    fdlStep2.left = new FormAttachment( 0, 0 );
    fdlStep2.right = new FormAttachment( middle, -margin );
    fdlStep2.top = new FormAttachment( wStep1, margin );
    wlStep2.setLayoutData( fdlStep2 );
    wStep2 = new CCombo( shell, SWT.BORDER );
    props.setLook( wStep2 );

    if ( previousSteps != null ) {
      wStep2.setItems( previousSteps );
    }

    wStep2.addModifyListener( lsMod );
    fdStep2 = new FormData();
    fdStep2.top = new FormAttachment( wStep1, margin );
    fdStep2.left = new FormAttachment( middle, 0 );
    fdStep2.right = new FormAttachment( 100, 0 );
    wStep2.setLayoutData( fdStep2 );

    // Join type
    wlType = new Label( shell, SWT.RIGHT );
    wlType.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Type.Label" ) );
    props.setLook( wlType );
    fdlType = new FormData();
    fdlType.left = new FormAttachment( 0, 0 );
    fdlType.right = new FormAttachment( middle, -margin );
    fdlType.top = new FormAttachment( wStep2, margin );
    wlType.setLayoutData( fdlType );
    wType = new CCombo( shell, SWT.BORDER );
    props.setLook( wType );

    wType.setItems( MergeJoinMeta.join_types );

    wType.addModifyListener( lsMod );
    fdType = new FormData();
    fdType.top = new FormAttachment( wStep2, margin );
    fdType.left = new FormAttachment( middle, 0 );
    fdType.right = new FormAttachment( 100, 0 );
    wType.setLayoutData( fdType );

    // THE KEYS TO MATCH for first step...
    wlKeys1 = new Label( shell, SWT.NONE );
    wlKeys1.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Keys1.Label" ) );
    props.setLook( wlKeys1 );
    fdlKeys1 = new FormData();
    fdlKeys1.left = new FormAttachment( 0, 0 );
    fdlKeys1.top = new FormAttachment( wType, margin );
    wlKeys1.setLayoutData( fdlKeys1 );

    int nrKeyRows1 = ( input.getKeyFields1() != null ? input.getKeyFields1().length : 1 );

    ColumnInfo[] ciKeys1 =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "MergeJoinDialog.ColumnInfo.KeyField1" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false ), };

    wKeys1 =
      new TableView(
        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKeys1,
        nrKeyRows1, lsMod, props );

    fdKeys1 = new FormData();
    fdKeys1.top = new FormAttachment( wlKeys1, margin );
    fdKeys1.left = new FormAttachment( 0, 0 );
    fdKeys1.bottom = new FormAttachment( 100, -70 );
    fdKeys1.right = new FormAttachment( 50, -margin );
    wKeys1.setLayoutData( fdKeys1 );

    wbKeys1 = new Button( shell, SWT.PUSH );
    wbKeys1.setText( BaseMessages.getString( PKG, "MergeJoinDialog.KeyFields1.Button" ) );
    fdbKeys1 = new FormData();
    fdbKeys1.top = new FormAttachment( wKeys1, margin );
    fdbKeys1.left = new FormAttachment( 0, 0 );
    fdbKeys1.right = new FormAttachment( 50, -margin );
    wbKeys1.setLayoutData( fdbKeys1 );
    wbKeys1.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        getKeys1();
      }
    } );

    // THE KEYS TO MATCH for second step
    wlKeys2 = new Label( shell, SWT.NONE );
    wlKeys2.setText( BaseMessages.getString( PKG, "MergeJoinDialog.Keys2.Label" ) );
    props.setLook( wlKeys2 );
    fdlKeys2 = new FormData();
    fdlKeys2.left = new FormAttachment( 50, 0 );
    fdlKeys2.top = new FormAttachment( wType, margin );
    wlKeys2.setLayoutData( fdlKeys2 );

    int nrKeyRows2 = ( input.getKeyFields2() != null ? input.getKeyFields2().length : 1 );

    ColumnInfo[] ciKeys2 =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "MergeJoinDialog.ColumnInfo.KeyField2" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false ), };

    wKeys2 =
      new TableView(
        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKeys2,
        nrKeyRows2, lsMod, props );

    fdKeys2 = new FormData();
    fdKeys2.top = new FormAttachment( wlKeys2, margin );
    fdKeys2.left = new FormAttachment( 50, 0 );
    fdKeys2.bottom = new FormAttachment( 100, -70 );
    fdKeys2.right = new FormAttachment( 100, 0 );
    wKeys2.setLayoutData( fdKeys2 );

    wbKeys2 = new Button( shell, SWT.PUSH );
    wbKeys2.setText( BaseMessages.getString( PKG, "MergeJoinDialog.KeyFields2.Button" ) );
    fdbKeys2 = new FormData();
    fdbKeys2.top = new FormAttachment( wKeys2, margin );
    fdbKeys2.left = new FormAttachment( 50, 0 );
    fdbKeys2.right = new FormAttachment( 100, 0 );
    wbKeys2.setLayoutData( fdbKeys2 );
    wbKeys2.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        getKeys2();
      }
    } );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wbKeys1 );

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
    List<StreamInterface> infoStreams = input.getStepIOMeta().getInfoStreams();

    wStep1.setText( Const.NVL( infoStreams.get( 0 ).getStepname(), "" ) );
    wStep2.setText( Const.NVL( infoStreams.get( 1 ).getStepname(), "" ) );
    String joinType = input.getJoinType();
    if ( joinType != null && joinType.length() > 0 ) {
      wType.setText( joinType );
    } else {
      wType.setText( MergeJoinMeta.join_types[ 0 ] );
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

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void getMeta( MergeJoinMeta meta ) {
    List<StreamInterface> infoStreams = meta.getStepIOMeta().getInfoStreams();

    infoStreams.get( 0 ).setStepMeta( transMeta.findStep( wStep1.getText() ) );
    infoStreams.get( 1 ).setStepMeta( transMeta.findStep( wStep2.getText() ) );
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
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    getMeta( input );

    // Show a warning (optional)
    //
    if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.DialogTitle" ),
          null,
          BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.DialogMessage", Const.CR ) + Const.CR,
          MessageDialog.WARNING,
          new String[] { BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.Option1" ) },
          0,
          BaseMessages.getString( PKG, "MergeJoinDialog.InputNeedSort.Option2" ), "N".equalsIgnoreCase(
          props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) );
      MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
      md.open();
      props.setCustomParameter( STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
      props.saveProps();
    }

    stepname = wStepname.getText(); // return value

    dispose();
  }

  private void getKeys1() {
    MergeJoinMeta joinMeta = new MergeJoinMeta();
    getMeta( joinMeta );

    try {
      List<StreamInterface> infoStreams = joinMeta.getStepIOMeta().getInfoStreams();

      StepMeta stepMeta = infoStreams.get( 0 ).getStepMeta();
      if ( stepMeta != null ) {
        RowMetaInterface prev = transMeta.getStepFields( stepMeta );
        if ( prev != null ) {
          BaseStepDialog.getFieldsFromPrevious( prev, wKeys1, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
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
      List<StreamInterface> infoStreams = joinMeta.getStepIOMeta().getInfoStreams();

      StepMeta stepMeta = infoStreams.get( 1 ).getStepMeta();
      if ( stepMeta != null ) {
        RowMetaInterface prev = transMeta.getStepFields( stepMeta );
        if ( prev != null ) {
          BaseStepDialog.getFieldsFromPrevious( prev, wKeys2, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
        }
      }
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MergeJoinDialog.ErrorGettingFields.DialogTitle" ), BaseMessages
        .getString( PKG, "MergeJoinDialog.ErrorGettingFields.DialogMessage" ), e );
    }
  }

}
