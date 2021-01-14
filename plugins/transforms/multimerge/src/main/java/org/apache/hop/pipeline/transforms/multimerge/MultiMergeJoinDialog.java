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

package org.apache.hop.pipeline.transforms.multimerge;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
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
import java.util.*;

public class MultiMergeJoinDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MultiMergeJoinMeta.class; // For Translator

  public static final String STRING_SORT_WARNING_PARAMETER = "MultiMergeJoinSortWarning";

  private final CCombo[] wInputTransformArray;
  private CCombo joinTypeCombo;
  private final Text[] keyValTextBox;

  private Map<String, Integer> inputFields;
  private IRowMeta prev;
  private ColumnInfo[] ciKeys;

  private final int margin = props.getMargin();

  private final MultiMergeJoinMeta joinMeta;

  public MultiMergeJoinDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    joinMeta = (MultiMergeJoinMeta) in;

    String[] inputTransformNames = getInputTransformNames();
    wInputTransformArray = new CCombo[ inputTransformNames.length ];
    keyValTextBox = new Text[ inputTransformNames.length ];
  }

  private String[] getInputTransformNames() {
    String[] inputTransformNames = joinMeta.getInputTransforms();
    ArrayList<String> nameList = new ArrayList<>();
    if ( inputTransformNames != null ) {
      Collections.addAll( nameList, inputTransformNames );
    }

    String[] prevTransformNames = pipelineMeta.getPrevTransformNames( transformName );
    if ( prevTransformNames != null ) {
      String prevTransformName;
      for (String name : prevTransformNames) {
        prevTransformName = name;
        if (nameList.contains(prevTransformName)) {
          continue;
        }
        nameList.add(prevTransformName);
      }
    }

    return nameList.toArray( new String[ nameList.size() ] );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransformDialog#open()
   */
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, joinMeta );

    final ModifyListener lsMod = e -> joinMeta.setChanged();
    backupChanged = joinMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MultiMergeJoinDialog.Shell.Label" ) );

    // int middle = props.getMiddlePct();

    wlTransformName = new Label( shell, SWT.LEFT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MultiMergeJoinDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( 15, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( 15, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 35, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // create widgets for input stream and join key selections
    createInputStreamWidgets( lsMod );

    // create widgets for Join type
    createJoinTypeWidget( lsMod );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    /*
     * lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
     *
     * wTransformName.addSelectionListener( lsDef );
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
    return transformName;
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
    if ( wInputTransformArray.length > 0 ) {
      fdlType.top = new FormAttachment( wInputTransformArray[ wInputTransformArray.length - 1 ], margin );
    } else {
      fdlType.top = new FormAttachment( wTransformName, margin );
    }
    joinTypeLabel.setLayoutData( fdlType );
    joinTypeCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( joinTypeCombo );

    joinTypeCombo.setItems( MultiMergeJoinMeta.joinTypes );

    joinTypeCombo.addModifyListener( lsMod );
    FormData fdType = new FormData();
    if ( wInputTransformArray.length > 0 ) {
      fdType.top = new FormAttachment( wInputTransformArray[ wInputTransformArray.length - 1 ], margin );
    } else {
      fdType.top = new FormAttachment( wTransformName, margin );
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
    // Get the previous transforms ...
    String[] inputTransforms = getInputTransformNames();
    for ( int index = 0; index < inputTransforms.length; index++ ) {
      Label wlTransform;
      FormData fdlTransform, fdTransform1;

      wlTransform = new Label( shell, SWT.LEFT );
      // wlTransform1.setText(+i+".Label"));
      wlTransform.setText( BaseMessages.getString( PKG, "MultiMergeJoinMeta.InputTransform" ) + ( index + 1 ) );
      props.setLook( wlTransform );
      fdlTransform = new FormData();
      fdlTransform.left = new FormAttachment( 0, 0 );
      fdlTransform.right = new FormAttachment( 15, -margin );
      if ( index == 0 ) {
        fdlTransform.top = new FormAttachment( wTransformName, margin );
      } else {
        fdlTransform.top = new FormAttachment( wInputTransformArray[ index - 1 ], margin );
      }

      wlTransform.setLayoutData( fdlTransform );
      wInputTransformArray[ index ] = new CCombo( shell, SWT.BORDER );
      props.setLook( wInputTransformArray[ index ] );

      wInputTransformArray[ index ].setItems( inputTransforms );

      wInputTransformArray[ index ].addModifyListener( lsMod );
      fdTransform1 = new FormData();
      fdTransform1.left = new FormAttachment( 15, 0 );
      if ( index == 0 ) {
        fdTransform1.top = new FormAttachment( wTransformName, margin );
      } else {
        fdTransform1.top = new FormAttachment( wInputTransformArray[ index - 1 ], margin );
      }

      fdTransform1.right = new FormAttachment( 35, 0 );
      wInputTransformArray[ index ].setLayoutData( fdTransform1 );

      Label keyLabel = new Label( shell, SWT.LEFT );
      keyLabel.setText( BaseMessages.getString( PKG, "MultiMergeJoinMeta.JoinKeys" ) );
      props.setLook( keyLabel );
      FormData keyTransform = new FormData();
      keyTransform.left = new FormAttachment( 35, margin + 5 );
      keyTransform.right = new FormAttachment( 45, -margin );
      if ( index == 0 ) {
        keyTransform.top = new FormAttachment( wTransformName, margin );
      } else {
        keyTransform.top = new FormAttachment( wInputTransformArray[ index - 1 ], margin );
      }
      keyLabel.setLayoutData( keyTransform );

      keyValTextBox[ index ] = new Text( shell, SWT.READ_ONLY | SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      props.setLook( keyValTextBox[ index ] );
      keyValTextBox[ index ].setText( "" );
      keyValTextBox[ index ].addModifyListener( lsMod );
      FormData keyData = new FormData();
      keyData.left = new FormAttachment( 45, margin + 5 );
      keyData.right = new FormAttachment( 65, -margin );
      if ( index == 0 ) {
        keyData.top = new FormAttachment( wTransformName, margin );
      } else {
        keyData.top = new FormAttachment( wInputTransformArray[ index - 1 ], margin );
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
        buttonData.top = new FormAttachment( wTransformName, margin );
      } else {
        buttonData.top = new FormAttachment( wInputTransformArray[ index - 1 ], margin );
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
    inputFields = new HashMap<>();

    final Shell subShell = new Shell( shell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    final FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 5;
    formLayout.marginHeight = 5;
    subShell.setLayout( formLayout );
    subShell.setSize( 200, 150 );
    subShell.setText( BaseMessages.getString( PKG, "MultiMergeJoinMeta.JoinKeys" ) );
    subShell.setImage( GuiResource.getInstance().getImagePipeline() );
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
      new TableView( variables, subShell, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKeys, nrKeyRows, lsMod, props );

    FormData fdKeys = new FormData();
    fdKeys.top = new FormAttachment( wlKeys, margin );
    fdKeys.left = new FormAttachment( 0, 0 );
    fdKeys.bottom = new FormAttachment( 100, -70 );
    fdKeys.right = new FormAttachment( 100, -margin );
    wKeys.setLayoutData( fdKeys );

    //
    // Search the fields in the background

    final Runnable runnable = () -> {
      try {
        CCombo wInputTransform = wInputTransformArray[ inputStreamIndex ];
        String transformName = wInputTransform.getText();
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta != null ) {
          prev = pipelineMeta.getTransformFields( variables, transformMeta );
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
        BaseTransformDialog.getFieldsFromPrevious( prev, wKeys, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
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
     * wTransformName.addSelectionListener( lsDef );
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
    final Map<String, Integer> fields = new HashMap<>();

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
    String[] inputTransformNames = joinMeta.getInputTransforms();
    if ( inputTransformNames != null ) {
      String inputTransformName;
      String[] keyFields = joinMeta.getKeyFields();
      String keyField;
      for ( int i = 0; i < inputTransformNames.length; i++ ) {
        inputTransformName = Const.NVL( inputTransformNames[ i ], "" );
        wInputTransformArray[ i ].setText( inputTransformName );

        keyField = Const.NVL( i < keyFields.length ? keyFields[ i ] : null, "" );
        keyValTextBox[ i ].setText( keyField );
      }

      String joinType = joinMeta.getJoinType();
      if ( joinType != null && joinType.length() > 0 ) {
        joinTypeCombo.setText( joinType );
      } else {
        joinTypeCombo.setText( MultiMergeJoinMeta.joinTypes[ 0 ] );
      }
    }
    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    joinMeta.setChanged( backupChanged );
    dispose();
  }

  /**
   * Get the meta data
   *
   * @param meta
   */
  private void getMeta( MultiMergeJoinMeta meta ) {
    ITransformIOMeta transformIOMeta = meta.getTransformIOMeta();
    List<IStream> infoStreams = transformIOMeta.getInfoStreams();
    IStream stream;
    String streamDescription;
    ArrayList<String> inputTransformNameList = new ArrayList<>();
    ArrayList<String> keyList = new ArrayList<>();
    CCombo wInputTransform;
    String inputTransformName;
    for ( int i = 0; i < wInputTransformArray.length; i++ ) {
      wInputTransform = wInputTransformArray[ i ];
      inputTransformName = wInputTransform.getText();

      if ( Utils.isEmpty( inputTransformName ) ) {
        continue;
      }

      inputTransformNameList.add( inputTransformName );
      keyList.add( keyValTextBox[ i ].getText() );

      if ( infoStreams.size() < inputTransformNameList.size() ) {
        streamDescription = BaseMessages.getString( PKG, "MultiMergeJoin.InfoStream.Description" );
        stream = new Stream( StreamType.INFO, null, streamDescription, StreamIcon.INFO, null );
        transformIOMeta.addStream( stream );
      }
    }

    int inputTransformCount = inputTransformNameList.size();
    meta.allocateInputTransforms( inputTransformCount );
    meta.allocateKeys( inputTransformCount );

    String[] inputTransforms = meta.getInputTransforms();
    String[] keyFields = meta.getKeyFields();
    infoStreams = transformIOMeta.getInfoStreams();
    for ( int i = 0; i < inputTransformCount; i++ ) {
      inputTransformName = inputTransformNameList.get( i );
      inputTransforms[ i ] = inputTransformName;
      stream = infoStreams.get( i );
      stream.setTransformMeta( pipelineMeta.findTransform( inputTransformName ) );
      keyFields[ i ] = keyList.get( i );
    }

    meta.setJoinType( joinTypeCombo.getText() );
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    getMeta( joinMeta );
    // Show a warning (optional)
    if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.DialogTitle" ),
          BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.DialogMessage", Const.CR ) + Const.CR,
          SWT.ICON_WARNING,
          new String[] { BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.Option1" ) },
          BaseMessages.getString( PKG, "MultiMergeJoinDialog.InputNeedSort.Option2" ),
          "N".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) );
      md.open();
      props.setCustomParameter( STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
    }
    transformName = wTransformName.getText(); // return value
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
