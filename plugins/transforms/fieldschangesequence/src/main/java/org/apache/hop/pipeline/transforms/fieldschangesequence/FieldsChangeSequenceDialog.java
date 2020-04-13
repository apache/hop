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

package org.apache.hop.pipeline.transforms.fieldschangesequence;

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
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class FieldsChangeSequenceDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = FieldsChangeSequenceMeta.class; // for i18n purposes, needed by Translator!!

  private FieldsChangeSequenceMeta input;

  private Label wlStart;
  private TextVar wStart;
  private FormData fdlStart, fdStart;

  private Label wlIncrement;
  private TextVar wIncrement;
  private FormData fdlIncrement, fdIncrement;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private Label wlResult;
  private Text wResult;
  private FormData fdlResult, fdResult;

  private Map<String, Integer> inputFields;

  private ColumnInfo[] colinf;

  public static final String STRING_CHANGE_SEQUENCE_WARNING_PARAMETER = "ChangeSequenceSortWarning";

  public FieldsChangeSequenceDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    input = (FieldsChangeSequenceMeta) in;
    inputFields = new HashMap<String, Integer>();
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
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.TransformName.Label" ) );
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

    // Result line...
    wlResult = new Label( shell, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.Result.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wTransformName, 2 * margin );
    wlResult.setLayoutData( fdlResult );
    wResult = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wTransformName, 2 * margin );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData( fdResult );

    // Start
    wlStart = new Label( shell, SWT.RIGHT );
    wlStart.setText( BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.Start.Label" ) );
    props.setLook( wlStart );
    fdlStart = new FormData();
    fdlStart.left = new FormAttachment( 0, 0 );
    fdlStart.right = new FormAttachment( middle, -margin );
    fdlStart.top = new FormAttachment( wResult, margin );
    wlStart.setLayoutData( fdlStart );
    wStart = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStart );
    fdStart = new FormData();
    fdStart.left = new FormAttachment( middle, 0 );
    fdStart.top = new FormAttachment( wResult, margin );
    fdStart.right = new FormAttachment( 100, 0 );
    wStart.setLayoutData( fdStart );

    // Increment
    wlIncrement = new Label( shell, SWT.RIGHT );
    wlIncrement.setText( BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.Increment.Label" ) );
    props.setLook( wlIncrement );
    fdlIncrement = new FormData();
    fdlIncrement.left = new FormAttachment( 0, 0 );
    fdlIncrement.right = new FormAttachment( middle, -margin );
    fdlIncrement.top = new FormAttachment( wStart, margin );
    wlIncrement.setLayoutData( fdlIncrement );
    wIncrement = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIncrement );
    fdIncrement = new FormData();
    fdIncrement.left = new FormAttachment( middle, 0 );
    fdIncrement.top = new FormAttachment( wStart, margin );
    fdIncrement.right = new FormAttachment( 100, 0 );
    wIncrement.setLayoutData( fdIncrement );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wGet, wCancel }, margin, null );

    // Table with fields
    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wIncrement, margin );
    wlFields.setLayoutData( fdlFields );

    final int FieldsCols = 1;
    final int FieldsRows = input.getFieldName().length;

    colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.Fieldname.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    wFields =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2 * margin );
    wFields.setLayoutData( fdFields );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

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

    //
    // Search the fields in the background
    //

    final Runnable runnable = new Runnable() {
      public void run() {
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta != null ) {
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields( transformMeta );
            if ( row != null ) {
              // Remember these fields...
              for ( int i = 0; i < row.size(); i++ ) {
                inputFields.put( row.getValueMeta( i ).getName(), new Integer( i ) );
              }

              setComboBoxes();
            }

            // Dislay in red missing field names
            Display.getDefault().asyncExec( new Runnable() {
              public void run() {
                if ( !wFields.isDisposed() ) {
                  for ( int i = 0; i < wFields.table.getItemCount(); i++ ) {
                    TableItem it = wFields.table.getItem( i );
                    if ( !Utils.isEmpty( it.getText( 1 ) ) ) {
                      if ( !inputFields.containsKey( it.getText( 1 ) ) ) {
                        it.setBackground( GuiResource.getInstance().getColorRed() );
                      }
                    }
                  }
                }
              }
            } );

          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.ErrorGettingPreviousFields", e
              .getMessage() ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

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
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    colinf[ 0 ].setComboValues( fieldNames );
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null ) {
        ITableItemInsertListener insertListener = new ITableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, IValueMeta v ) {
            tableItem.setText( 2, BaseMessages.getString( PKG, "System.Combo.Yes" ) );
            return true;
          }
        };
        BaseTransformDialog
          .getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, insertListener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wStart.setText( Const.NVL( input.getStart(), "1" ) );
    wIncrement.setText( Const.NVL( input.getIncrement(), "1" ) );
    wResult.setText( Const.NVL( input.getResultFieldName(), "result" ) );

    Table table = wFields.table;
    if ( input.getFieldName().length > 0 ) {
      table.removeAll();
    }
    for ( int i = 0; i < input.getFieldName().length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      ti.setText( 1, input.getFieldName()[ i ] );
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

    input.setStart( wStart.getText() );
    input.setIncrement( wIncrement.getText() );
    input.setResultFieldName( wResult.getText() );

    int nrFields = wFields.nrNonEmpty();
    input.allocate( nrFields );
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem ti = wFields.getNonEmpty( i );
      //CHECKSTYLE:Indentation:OFF
      input.getFieldName()[ i ] = ti.getText( 1 );
    }

    if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_CHANGE_SEQUENCE_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.InputNeedSort.DialogTitle" ),
          null,
          BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.InputNeedSort.DialogMessage", Const.CR ) + Const.CR,
          MessageDialog.WARNING,
          new String[] { BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.InputNeedSort.Option1" ) },
          0,
          BaseMessages.getString( PKG, "FieldsChangeSequenceDialog.InputNeedSort.Option2" ), "N".equalsIgnoreCase(
          props.getCustomParameter( STRING_CHANGE_SEQUENCE_WARNING_PARAMETER, "Y" ) ) );
      MessageDialogWithToggle.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
      md.open();
      props.setCustomParameter( STRING_CHANGE_SEQUENCE_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
      props.saveProps();
    }

    dispose();
  }
}
