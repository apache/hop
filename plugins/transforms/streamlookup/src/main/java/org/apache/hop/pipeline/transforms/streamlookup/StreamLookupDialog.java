/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.streamlookup;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.dialog.ErrorDialog;
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

public class StreamLookupDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = StreamLookupMeta.class; // for i18n purposes, needed by Translator!!

  private Label wlTransform;
  private CCombo wTransform;
  private FormData fdlTransform, fdTransform;

  private Label wlKey;
  private TableView wKey;
  private FormData fdlKey, fdKey;

  private Label wlReturn;
  private TableView wReturn;
  private FormData fdlReturn, fdReturn;

  private Label wlPreserveMemory;
  private Button wPreserveMemory;
  private FormData fdlPreserveMemory, fdPreserveMemory;

  private Label wlSortedList;
  private Button wSortedList;
  private FormData fdlSortedList, fdSortedList;

  private Label wlIntegerPair;
  private Button wIntegerPair;
  private FormData fdlIntegerPair, fdIntegerPair;

  private StreamLookupMeta input;

  private Button wGetLU;
  private Listener lsGetLU;

  private ColumnInfo[] ciKey;

  private ColumnInfo[] ciReturn;

  public StreamLookupDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (StreamLookupMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    SelectionListener lsSelection = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setComboBoxesLookup();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "StreamLookupDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "StreamLookupDialog.TransformName.Label" ) );
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

    // Lookup transform line...
    wlTransform = new Label( shell, SWT.RIGHT );
    wlTransform.setText( BaseMessages.getString( PKG, "StreamLookupDialog.LookupTransform.Label" ) );
    props.setLook( wlTransform );
    fdlTransform = new FormData();
    fdlTransform.left = new FormAttachment( 0, 0 );
    fdlTransform.right = new FormAttachment( middle, -margin );
    fdlTransform.top = new FormAttachment( wTransformName, margin * 2 );
    wlTransform.setLayoutData( fdlTransform );
    wTransform = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransform );

    List<TransformMeta> previousTransforms = pipelineMeta.findPreviousTransforms( transformMeta, true );
    for ( TransformMeta previousTransform : previousTransforms ) {
      wTransform.add( previousTransform.getName() );
    }
    // pipelineMeta.getInfoTransform()

    wTransform.addModifyListener( lsMod );
    wTransform.addSelectionListener( lsSelection );

    fdTransform = new FormData();
    fdTransform.left = new FormAttachment( middle, 0 );
    fdTransform.top = new FormAttachment( wTransformName, margin * 2 );
    fdTransform.right = new FormAttachment( 100, 0 );
    wTransform.setLayoutData( fdTransform );

    wlKey = new Label( shell, SWT.NONE );
    wlKey.setText( BaseMessages.getString( PKG, "StreamLookupDialog.Key.Label" ) );
    props.setLook( wlKey );
    fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.top = new FormAttachment( wTransform, margin );
    wlKey.setLayoutData( fdlKey );

    int nrKeyCols = 2;
    int nrKeyRows = ( input.getKeystream() != null ? input.getKeystream().length : 1 );

    ciKey = new ColumnInfo[ nrKeyCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StreamLookupDialog.ColumnInfo.Field" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    ciKey[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StreamLookupDialog.ColumnInfo.LookupField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );

    wKey =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
        nrKeyRows, lsMod, props );

    fdKey = new FormData();
    fdKey.left = new FormAttachment( 0, 0 );
    fdKey.top = new FormAttachment( wlKey, margin );
    fdKey.right = new FormAttachment( 100, 0 );
    fdKey.bottom = new FormAttachment( wlKey, 180 );
    wKey.setLayoutData( fdKey );

    // THE UPDATE/INSERT TABLE
    wlReturn = new Label( shell, SWT.NONE );
    wlReturn.setText( BaseMessages.getString( PKG, "StreamLookupDialog.ReturnFields.Label" ) );
    props.setLook( wlReturn );
    fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment( wKey, margin );
    wlReturn.setLayoutData( fdlReturn );

    int UpInsCols = 4;
    int UpInsRows = ( input.getValue() != null ? input.getValue().length : 1 );

    ciReturn = new ColumnInfo[ UpInsCols ];
    ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StreamLookupDialog.ColumnInfo.FieldReturn" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StreamLookupDialog.ColumnInfo.NewName" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    ciReturn[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StreamLookupDialog.ColumnInfo.Default" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    ciReturn[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "StreamLookupDialog.ColumnInfo.Type" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        ValueMetaFactory.getValueMetaNames() );

    wReturn =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn,
        UpInsRows, lsMod, props );

    // START MEMORY PRESERVE GROUP

    fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment( wlReturn, margin );
    fdReturn.right = new FormAttachment( 100, 0 );
    fdReturn.bottom = new FormAttachment( 100, -125 );
    wReturn.setLayoutData( fdReturn );

    wlPreserveMemory = new Label( shell, SWT.RIGHT );
    wlPreserveMemory.setText( BaseMessages.getString( PKG, "StreamLookupDialog.PreserveMemory.Label" ) );
    props.setLook( wlPreserveMemory );
    fdlPreserveMemory = new FormData();
    fdlPreserveMemory.left = new FormAttachment( 0, 0 );
    fdlPreserveMemory.top = new FormAttachment( wReturn, margin );
    fdlPreserveMemory.right = new FormAttachment( middle, -margin );
    wlPreserveMemory.setLayoutData( fdlPreserveMemory );
    wPreserveMemory = new Button( shell, SWT.CHECK );
    props.setLook( wPreserveMemory );
    fdPreserveMemory = new FormData();
    fdPreserveMemory.left = new FormAttachment( middle, 0 );
    fdPreserveMemory.top = new FormAttachment( wReturn, margin );
    fdPreserveMemory.right = new FormAttachment( 100, 0 );
    wPreserveMemory.setLayoutData( fdPreserveMemory );
    wPreserveMemory.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    wlIntegerPair = new Label( shell, SWT.RIGHT );
    wlIntegerPair.setText( BaseMessages.getString( PKG, "StreamLookupDialog.IntegerPair.Label" ) );
    props.setLook( wlIntegerPair );
    fdlIntegerPair = new FormData();
    fdlIntegerPair.left = new FormAttachment( 0, 0 );
    fdlIntegerPair.top = new FormAttachment( wPreserveMemory, margin );
    fdlIntegerPair.right = new FormAttachment( middle, -margin );
    wlIntegerPair.setLayoutData( fdlIntegerPair );
    wIntegerPair = new Button( shell, SWT.RADIO );
    wIntegerPair.setEnabled( false );
    props.setLook( wIntegerPair );
    fdIntegerPair = new FormData();
    fdIntegerPair.left = new FormAttachment( middle, 0 );
    fdIntegerPair.top = new FormAttachment( wPreserveMemory, margin );
    fdIntegerPair.right = new FormAttachment( 100, 0 );
    wIntegerPair.setLayoutData( fdIntegerPair );
    wIntegerPair.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    wlSortedList = new Label( shell, SWT.RIGHT );
    wlSortedList.setText( BaseMessages.getString( PKG, "StreamLookupDialog.SortedList.Label" ) );
    props.setLook( wlSortedList );
    fdlSortedList = new FormData();
    fdlSortedList.left = new FormAttachment( 0, 0 );
    fdlSortedList.top = new FormAttachment( wIntegerPair, margin );
    fdlSortedList.right = new FormAttachment( middle, -margin );
    wlSortedList.setLayoutData( fdlSortedList );
    wSortedList = new Button( shell, SWT.RADIO );
    wSortedList.setEnabled( false );
    props.setLook( wSortedList );
    fdSortedList = new FormData();
    fdSortedList.left = new FormAttachment( middle, 0 );
    fdSortedList.top = new FormAttachment( wIntegerPair, margin );
    fdSortedList.right = new FormAttachment( 100, 0 );
    wSortedList.setLayoutData( fdSortedList );
    wSortedList.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );
    // PDI-2107 preserve memory should be enabled to have this options on.
    wPreserveMemory.addListener( SWT.Selection, new Listener() {
      @Override
      public void handleEvent( Event event ) {
        boolean selection = wPreserveMemory.getSelection();
        wSortedList.setEnabled( selection );
        wIntegerPair.setEnabled( selection );
      }
    } );

    // END MEMORY PRESERVE

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "StreamLookupDialog.GetFields.Button" ) );
    wGetLU = new Button( shell, SWT.PUSH );
    wGetLU.setText( BaseMessages.getString( PKG, "StreamLookupDialog.GetLookupFields.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel, wGet, wGetLU }, margin, null );

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsGetLU = new Listener() {
      public void handleEvent( Event e ) {
        getlookup();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wGet.addListener( SWT.Selection, lsGet );
    wGetLU.addListener( SWT.Selection, lsGetLU );
    wCancel.addListener( SWT.Selection, lsCancel );

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

    setComboBoxes();
    setComboBoxesLookup();
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
    //
    // Search the fields in the background
    //

    final Runnable runnable = new Runnable() {
      public void run() {
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta != null ) {
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields( transformMeta );
            Map<String, Integer> prevFields = new HashMap<String, Integer>();
            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              prevFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }

            // Something was changed in the row.
            //
            final Map<String, Integer> fields = new HashMap<String, Integer>();

            // Add the currentMeta fields...
            fields.putAll( prevFields );

            Set<String> keySet = fields.keySet();
            List<String> entries = new ArrayList<>( keySet );

            String[] fieldNames = entries.toArray( new String[ entries.size() ] );
            Const.sortStrings( fieldNames );
            // return fields
            ciKey[ 0 ].setComboValues( fieldNames );
          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

  }

  protected void setComboBoxesLookup() {
    Runnable fieldLoader = new Runnable() {
      public void run() {
        TransformMeta lookupTransformMeta = pipelineMeta.findTransform( wTransform.getText() );
        if ( lookupTransformMeta != null ) {
          try {
            IRowMeta row = pipelineMeta.getTransformFields( lookupTransformMeta );
            Map<String, Integer> lookupFields = new HashMap<String, Integer>();
            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              lookupFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }

            // Something was changed in the row.
            //
            final Map<String, Integer> fields = new HashMap<String, Integer>();

            // Add the currentMeta fields...
            fields.putAll( lookupFields );

            Set<String> keySet = fields.keySet();
            List<String> entries = new ArrayList<>( keySet );

            String[] fieldNames = entries.toArray( new String[ entries.size() ] );
            Const.sortStrings( fieldNames );
            // return fields
            ciReturn[ 0 ].setComboValues( fieldNames );
            ciKey[ 1 ].setComboValues( fieldNames );
          } catch ( HopException e ) {
            logError( "It was not possible to retrieve the list of fields for transform [" + wTransform.getText() + "]!" );
          }
        }
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StreamLookupDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getKeystream() != null ) {
      for ( int i = 0; i < input.getKeystream().length; i++ ) {
        TableItem item = wKey.table.getItem( i );
        if ( input.getKeystream()[ i ] != null ) {
          item.setText( 1, input.getKeystream()[ i ] );
        }
        if ( input.getKeylookup()[ i ] != null ) {
          item.setText( 2, input.getKeylookup()[ i ] );
        }
      }
    }

    if ( input.getValue() != null ) {
      for ( int i = 0; i < input.getValue().length; i++ ) {
        TableItem item = wReturn.table.getItem( i );
        if ( input.getValue()[ i ] != null ) {
          item.setText( 1, input.getValue()[ i ] );
        }
        if ( input.getValueName()[ i ] != null && !input.getValueName()[ i ].equals( input.getValue()[ i ] ) ) {
          item.setText( 2, input.getValueName()[ i ] );
        }
        if ( input.getValueDefault()[ i ] != null ) {
          item.setText( 3, input.getValueDefault()[ i ] );
        }
        item.setText( 4, ValueMetaFactory.getValueMetaName( input.getValueDefaultType()[ i ] ) );
      }
    }

    IStream infoStream = input.getTransformIOMeta().getInfoStreams().get( 0 );
    wTransform.setText( Const.NVL( infoStream.getTransformName(), "" ) );

    boolean isPreserveMemory = input.isMemoryPreservationActive();
    wPreserveMemory.setSelection( isPreserveMemory );
    if ( isPreserveMemory ) {
      wSortedList.setEnabled( true );
      wIntegerPair.setEnabled( true );
    }
    // PDI-2107 usually this is sorted list or integer pair
    // for backward compatibility they can be set both
    // but user will be forced to choose only one option later.
    wSortedList.setSelection( input.isUsingSortedList() );
    wIntegerPair.setSelection( input.isUsingIntegerPair() );

    wKey.setRowNums();
    wKey.optWidth( true );
    wReturn.setRowNums();
    wReturn.optWidth( true );

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

    int nrkeys = wKey.nrNonEmpty();
    int nrvalues = wReturn.nrNonEmpty();
    input.allocate( nrkeys, nrvalues );
    input.setMemoryPreservationActive( wPreserveMemory.getSelection() );
    input.setUsingSortedList( wSortedList.getSelection() );
    input.setUsingIntegerPair( wIntegerPair.getSelection() );

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StreamLookupDialog.Log.FoundKeys", nrkeys + "" ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrkeys; i++ ) {
      TableItem item = wKey.getNonEmpty( i );
      input.getKeystream()[ i ] = item.getText( 1 );
      input.getKeylookup()[ i ] = item.getText( 2 );
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StreamLookupDialog.Log.FoundFields", nrvalues + "" ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrvalues; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      input.getValue()[ i ] = item.getText( 1 );
      input.getValueName()[ i ] = item.getText( 2 );
      if ( input.getValueName()[ i ] == null || input.getValueName()[ i ].length() == 0 ) {
        input.getValueName()[ i ] = input.getValue()[ i ];
      }
      input.getValueDefault()[ i ] = item.getText( 3 );
      input.getValueDefaultType()[ i ] = ValueMetaFactory.getIdForValueMeta( item.getText( 4 ) );
    }

    IStream infoStream = input.getTransformIOMeta().getInfoStreams().get( 0 );
    infoStream.setTransformMeta( pipelineMeta.findTransform( wTransform.getText() ) );
    if ( infoStream.getTransformMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      if ( Utils.isEmpty( wTransform.getText() ) ) {
        mb.setMessage( BaseMessages.getString( PKG, "StreamLookupDialog.NotTransformSpecified.DialogMessage", wTransform
          .getText() ) );
      } else {
        mb.setMessage( BaseMessages.getString( PKG, "StreamLookupDialog.TransformCanNotFound.DialogMessage", wTransform
          .getText() ) );
      }

      mb.setText( BaseMessages.getString( PKG, "StreamLookupDialog.TransformCanNotFound.DialogTitle" ) );
      mb.open();
    }

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if ( pipelineMeta.findTransform( wTransform.getText() ) == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb
        .setMessage( BaseMessages
          .getString( PKG, "StreamLookupDialog.PleaseSelectATransformToReadFrom.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "StreamLookupDialog.PleaseSelectATransformToReadFrom.DialogTitle" ) );
      mb.open();
      return;
    }

    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious( r, wKey, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
      } else {
        String transformFrom = wTransform.getText();
        if ( !Utils.isEmpty( transformFrom ) ) {
          r = pipelineMeta.getTransformFields( transformFrom );
          if ( r != null ) {
            BaseTransformDialog.getFieldsFromPrevious( r, wKey, 2, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "StreamLookupDialog.CouldNotFindFields.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "StreamLookupDialog.CouldNotFindFields.DialogTitle" ) );
            mb.open();
          }
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "StreamLookupDialog.TransformNameRequired.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "StreamLookupDialog.TransformNameRequired.DialogTitle" ) );
          mb.open();
        }
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "StreamLookupDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "StreamLookupDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void getlookup() {
    try {
      String transformFrom = wTransform.getText();
      if ( !Utils.isEmpty( transformFrom ) ) {
        IRowMeta r = pipelineMeta.getTransformFields( transformFrom );
        if ( r != null && !r.isEmpty() ) {
          BaseTransformDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1 }, new int[] { 4 }, -1, -1, null );
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "StreamLookupDialog.CouldNotFindFields.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "StreamLookupDialog.CouldNotFindFields.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( BaseMessages.getString( PKG, "StreamLookupDialog.TransformNameRequired.DialogMessage" ) );
        mb.setText( BaseMessages.getString( PKG, "StreamLookupDialog.TransformNameRequired.DialogTitle" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "StreamLookupDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "StreamLookupDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }
}
