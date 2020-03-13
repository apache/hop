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

package org.apache.hop.ui.trans.steps.multimerge;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepIOMetaInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.errorhandling.Stream;
import org.apache.hop.trans.step.errorhandling.StreamIcon;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.trans.step.errorhandling.StreamInterface.StreamType;
import org.apache.hop.trans.steps.multimerge.MultiMergeJoinMeta;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiMergeJoinDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = MultiMergeJoinMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String STRING_SORT_WARNING_PARAMETER = "MultiMergeJoinSortWarning";

  private CCombo[] wInputStepArray;
  private CCombo joinTypeCombo;
  private Text[] keyValTextBox;

  private Map<String, Integer> inputFields;
  private RowMetaInterface prev;
  private ColumnInfo[] ciKeys;

  private static final int margin = props.getMargin();

  private MultiMergeJoinMeta joinMeta;

  public MultiMergeJoinDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    joinMeta = (MultiMergeJoinMeta) in;

    String[] inputStepNames = getInputStepNames();
    wInputStepArray = new CCombo[ inputStepNames.length ];
    keyValTextBox = new Text[ inputStepNames.length ];
  }

  private String[] getInputStepNames() {
    String[] inputStepNames = joinMeta.getInputSteps();
    ArrayList<String> nameList = new ArrayList<>();
    if ( inputStepNames != null ) {
      Collections.addAll( nameList, inputStepNames );
    }

    String[] prevStepNames = transMeta.getPrevStepNames( stepname );
    if ( prevStepNames != null ) {
      String prevStepName;
      for ( int i = 0; i < prevStepNames.length; i++ ) {
        prevStepName = prevStepNames[ i ];
        if ( nameList.contains( prevStepName ) ) {
          continue;
        }
        nameList.add( prevStepName );
      }
    }

    return nameList.toArray( new String[ nameList.size() ] );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.trans.step.StepDialogInterface#open()
   */
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, joinMeta );

    final ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        joinMeta.setChanged();
      }
    };
    backupChanged = joinMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MultiMergeJoinDialog.Shell.Label" ) );

    // int middle = props.getMiddlePct();

    wlStepname = new Label( shell, SWT.LEFT );
    wlStepname.setText( BaseMessages.getString( PKG, "MultiMergeJoinDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( 15, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( 15, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 35, 0 );
    wStepname.setLayoutData( fdStepname );

    // create widgets for input stream and join key selections
    createInputStreamWidgets( lsMod );

    // create widgets for Join type
    createJoinTypeWidget( lsMod );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

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

    /*
     * lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
     *
     * wStepname.addSelectionListener( lsDef );
     */

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    // get the data
    getData();
    joinMeta.setChanged( backupChanged );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Create widgets for join type selection
   *
   * @param lsMod
   */
  private void createJoinTypeWidget( final ModifyListener lsMod ) {
    Label joinTypeLabel = new Label( shell, SWT.LEFT );
    joinTypeLabel.setText( BaseMessages.getString( PKG, "MultiMergeJoinDialog.Type.Label" ) );
    props.setLook( joinTypeLabel );
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment( 0, 0 );
    fdlType.right = new FormAttachment( 15, -margin );
    if ( wInputStepArray.length > 0 ) {
      fdlType.top = new FormAttachment( wInputStepArray[ wInputStepArray.length - 1 ], margin );
    } else {
      fdlType.top = new FormAttachment( wStepname, margin );
    }
    joinTypeLabel.setLayoutData( fdlType );
    joinTypeCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( joinTypeCombo );

    joinTypeCombo.setItems( MultiMergeJoinMeta.join_types );

    joinTypeCombo.addModifyListener( lsMod );
    FormData fdType = new FormData();
    if ( wInputStepArray.length > 0 ) {
      fdType.top = new FormAttachment( wInputStepArray[ wInputStepArray.length - 1 ], margin );
    } else {
      fdType.top = new FormAttachment( wStepname, margin );
    }
    fdType.left = new FormAttachment( 15, 0 );
    fdType.right = new FormAttachment( 35, 0 );
    joinTypeCombo.setLayoutData( fdType );
  }

  /**
   * create widgets for input stream and join keys
   *
   * @param lsMod
   */
  private void createInputStreamWidgets( final ModifyListener lsMod ) {
    // Get the previous steps ...
    String[] inputSteps = getInputStepNames();
    for ( int index = 0; index < inputSteps.length; index++ ) {
      Label wlStep;
      FormData fdlStep, fdStep1;

      wlStep = new Label( shell, SWT.LEFT );
      // wlStep1.setText(+i+".Label"));
      wlStep.setText( BaseMessages.getString( PKG, "MultiMergeJoinMeta.InputStep" ) + ( index + 1 ) );
      props.setLook( wlStep );
      fdlStep = new FormData();
      fdlStep.left = new FormAttachment( 0, 0 );
      fdlStep.right = new FormAttachment( 15, -margin );
      if ( index == 0 ) {
        fdlStep.top = new FormAttachment( wStepname, margin );
      } else {
        fdlStep.top = new FormAttachment( wInputStepArray[ index - 1 ], margin );
      }

      wlStep.setLayoutData( fdlStep );
      wInputStepArray[ index ] = new CCombo( shell, SWT.BORDER );
      props.setLook( wInputStepArray[ index ] );

      wInputStepArray[ index ].setItems( inputSteps );

      wInputStepArray[ index ].addModifyListener( lsMod );
      fdStep1 = new FormData();
      fdStep1.left = new FormAttachment( 15, 0 );
      if ( index == 0 ) {
        fdStep1.top = new FormAttachment( wStepname, margin );
      } else {
        fdStep1.top = new FormAttachment( wInputStepArray[ index - 1 ], margin );
      }

      fdStep1.right = new FormAttachment( 35, 0 );
      wInputStepArray[ index ].setLayoutData( fdStep1 );

      Label keyLabel = new Label( shell, SWT.LEFT );
      keyLabel.setText( BaseMessages.getString( PKG, "MultiMergeJoinMeta.JoinKeys" ) );
      props.setLook( keyLabel );
      FormData keyStep = new FormData();
      keyStep.left = new FormAttachment( 35, margin + 5 );
      keyStep.right = new FormAttachment( 45, -margin );
      if ( index == 0 ) {
        keyStep.top = new FormAttachment( wStepname, margin );
      } else {
        keyStep.top = new FormAttachment( wInputStepArray[ index - 1 ], margin );
      }
      keyLabel.setLayoutData( keyStep );

      keyValTextBox[ index ] = new Text( shell, SWT.READ_ONLY | SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      props.setLook( keyValTextBox[ index ] );
      keyValTextBox[ index ].setText( "" );
      keyValTextBox[ index ].addModifyListener( lsMod );
      FormData keyData = new FormData();
      keyData.left = new FormAttachment( 45, margin + 5 );
      keyData.right = new FormAttachment( 65, -margin );
      if ( index == 0 ) {
        keyData.top = new FormAttachment( wStepname, margin );
      } else {
        keyData.top = new FormAttachment( wInputStepArray[ index - 1 ], margin );
      }
      keyValTextBox[ index ].setLayoutData( keyData );

      Button button = new Button( shell, SWT.PUSH );
      button.setText( BaseMessages.getString( PKG, "MultiMergeJoinMeta.SelectKeys" ) );
      // add listener
      button
        .addListener( SWT.Selection, new ConfigureKeyButtonListener( this, keyValTextBox[ index ], index, lsMod ) );
      FormData buttonData = new FormData();
      buttonData.left = new FormAttachment( 65, margin );
      buttonData.right = new FormAttachment( 80, -margin );
      if ( index == 0 ) {
        buttonData.top = new FormAttachment( wStepname, margin );
      } else {
        buttonData.top = new FormAttachment( wInputStepArray[ index - 1 ], margin );
      }
      button.setLayoutData( buttonData );
    }
  }

  /**
   * "Configure join key" shell
   *
   * @param keyValTextBox
   * @param lsMod
   */
  private void configureKeys( final Text keyValTextBox, final int inputStreamIndex, ModifyListener lsMod ) {
    inputFields = new HashMap<String, Integer>();

    final Shell subShell = new Shell( shell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    final FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 5;
    formLayout.marginHeight = 5;
    subShell.setLayout( formLayout );
    subShell.setSize( 200, 150 );
    subShell.setText( BaseMessages.getString( PKG, "MultiMergeJoinMeta.JoinKeys" ) );
    subShell.setImage( GUIResource.getInstance().getImageTransGraph() );
    Label wlKeys = new Label( subShell, SWT.NONE );
    wlKeys.setText( BaseMessages.getString( PKG, "MultiMergeJoinDialog.Keys" ) );
    FormData fdlKeys = new FormData();
    fdlKeys.left = new FormAttachment( 0, 0 );
    fdlKeys.right = new FormAttachment( 50, -margin );
    fdlKeys.top = new FormAttachment( 0, margin );
    wlKeys.setLayoutData( fdlKeys );

    String[] keys = keyValTextBox.getText().split( "," );
    int nrKeyRows = ( keys != null ? keys.length : 1 );

    ciKeys =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "MultiMergeJoinDialog.ColumnInfo.KeyField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ), };

    final TableView wKeys =
      new TableView( transMeta, subShell, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKeys, nrKeyRows, lsMod, props );

    FormData fdKeys = new FormData();
    fdKeys.top = new FormAttachment( wlKeys, margin );
    fdKeys.left = new FormAttachment( 0, 0 );
    fdKeys.bottom = new FormAttachment( 100, -70 );
    fdKeys.right = new FormAttachment( 100, -margin );
    wKeys.setLayoutData( fdKeys );

    //
    // Search the fields in the background

    final Runnable runnable = new Runnable() {
      public void run() {
        try {
          CCombo wInputStep = wInputStepArray[ inputStreamIndex ];
          String stepName = wInputStep.getText();
          StepMeta stepMeta = transMeta.findStep( stepName );
          if ( stepMeta != null ) {
            prev = transMeta.getStepFields( stepMeta );
            if ( prev != null ) {
              // Remember these fields...
              for ( int i = 0; i < prev.size(); i++ ) {
                inputFields.put( prev.getValueMeta( i ).getName(), Integer.valueOf( i ) );
              }
              setComboBoxes();
            }
          }
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    Display.getDefault().asyncExec( runnable );

    Button getKeyButton = new Button( subShell, SWT.PUSH );
    getKeyButton.setText( BaseMessages.getString( PKG, "MultiMergeJoinDialog.KeyFields.Button" ) );
    FormData fdbKeys = new FormData();
    fdbKeys.top = new FormAttachment( wKeys, margin );
    fdbKeys.left = new FormAttachment( 0, 0 );
    fdbKeys.right = new FormAttachment( 100, -margin );
    getKeyButton.setLayoutData( fdbKeys );
    getKeyButton.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        BaseStepDialog.getFieldsFromPrevious( prev, wKeys, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
      }
    } );
    // Some buttons
    Button okButton = new Button( subShell, SWT.PUSH );
    okButton.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    setButtonPositions( new Button[] { okButton }, margin, getKeyButton );

    okButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        int nrKeys = wKeys.nrNonEmpty();
        StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < nrKeys; i++ ) {
          TableItem item = wKeys.getNonEmpty( i );
          sb.append( item.getText( 1 ) );
          if ( nrKeys > 1 && i != nrKeys - 1 ) {
            sb.append( "," );
          }
        }
        keyValTextBox.setText( sb.toString() );
        subShell.close();
      }
    } );

    /*
     * SelectionAdapter lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
     *
     * wStepname.addSelectionListener( lsDef );
     */

    // Detect X or ALT-F4 or something that kills this window...
    subShell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {

      }
    } );

    for ( int i = 0; i < keys.length; i++ ) {
      TableItem item = wKeys.table.getItem( i );
      if ( keys[ i ] != null ) {
        item.setText( 1, keys[ i ] );
      }
    }

    subShell.pack();
    subShell.open();

  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    ciKeys[ 0 ].setComboValues( fieldNames );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    String[] inputStepNames = joinMeta.getInputSteps();
    if ( inputStepNames != null ) {
      String inputStepName;
      String[] keyFields = joinMeta.getKeyFields();
      String keyField;
      for ( int i = 0; i < inputStepNames.length; i++ ) {
        inputStepName = Const.NVL( inputStepNames[ i ], "" );
        wInputStepArray[ i ].setText( inputStepName );

        keyField = Const.NVL( i < keyFields.length ? keyFields[ i ] : null, "" );
        keyValTextBox[ i ].setText( keyField );
      }

      String joinType = joinMeta.getJoinType();
      if ( joinType != null && joinType.length() > 0 ) {
        joinTypeCombo.setText( joinType );
      } else {
        joinTypeCombo.setText( MultiMergeJoinMeta.join_types[ 0 ] );
      }
    }
    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    joinMeta.setChanged( backupChanged );
    dispose();
  }

  /**
   * Get the meta data
   *
   * @param meta
   */
  private void getMeta( MultiMergeJoinMeta meta ) {
    StepIOMetaInterface stepIOMeta = meta.getStepIOMeta();
    List<StreamInterface> infoStreams = stepIOMeta.getInfoStreams();
    StreamInterface stream;
    String streamDescription;
    ArrayList<String> inputStepNameList = new ArrayList<>();
    ArrayList<String> keyList = new ArrayList<>();
    CCombo wInputStep;
    String inputStepName;
    for ( int i = 0; i < wInputStepArray.length; i++ ) {
      wInputStep = wInputStepArray[ i ];
      inputStepName = wInputStep.getText();

      if ( Utils.isEmpty( inputStepName ) ) {
        continue;
      }

      inputStepNameList.add( inputStepName );
      keyList.add( keyValTextBox[ i ].getText() );

      if ( infoStreams.size() < inputStepNameList.size() ) {
        streamDescription = BaseMessages.getString( PKG, "MultiMergeJoin.InfoStream.Description" );
        stream = new Stream( StreamType.INFO, null, streamDescription, StreamIcon.INFO, null );
        stepIOMeta.addStream( stream );
      }
    }

    int inputStepCount = inputStepNameList.size();
    meta.allocateInputSteps( inputStepCount );
    meta.allocateKeys( inputStepCount );

    String[] inputSteps = meta.getInputSteps();
    String[] keyFields = meta.getKeyFields();
    infoStreams = stepIOMeta.getInfoStreams();
    for ( int i = 0; i < inputStepCount; i++ ) {
      inputStepName = inputStepNameList.get( i );
      inputSteps[ i ] = inputStepName;
      stream = infoStreams.get( i );
      stream.setStepMeta( transMeta.findStep( inputStepName ) );
      keyFields[ i ] = keyList.get( i );
    }

    meta.setJoinType( joinTypeCombo.getText() );
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    getMeta( joinMeta );
    // Show a warning (optional)
    if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.DialogTitle" ),
          null,
          BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.DialogMessage", Const.CR ) + Const.CR,
          MessageDialog.WARNING,
          new String[] { BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.Option1" ) },
          0,
          BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.Option2" ),
          "N".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) );
      MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
      md.open();
      props.setCustomParameter( STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
      props.saveProps();
    }
    stepname = wStepname.getText(); // return value
    dispose();
  }

  /**
   * Listener for Configure Keys button
   *
   * @author A71481
   */
  private static class ConfigureKeyButtonListener implements Listener {
    MultiMergeJoinDialog dialog;
    Text textBox;
    int inputStreamIndex;
    ModifyListener listener;

    public ConfigureKeyButtonListener( MultiMergeJoinDialog dialog, Text textBox, int streamIndex,
                                       ModifyListener lsMod ) {
      this.dialog = dialog;
      this.textBox = textBox;
      this.listener = lsMod;
      this.inputStreamIndex = streamIndex;
    }

    @Override
    public void handleEvent( Event event ) {
      dialog.configureKeys( textBox, inputStreamIndex, listener );
    }

  }

}
