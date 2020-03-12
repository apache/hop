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

package org.apache.hop.trans.steps.sql;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.apache.hop.ui.trans.steps.tableinput.SQLValuesHighlight;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class ExecSQLDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = ExecSQLMeta.class; // for i18n purposes, needed by Translator2!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlSQL;

  private StyledTextComp wSQL;

  private FormData fdlSQL, fdSQL;

  private Label wlEachRow;
  private Button wEachRow;

  private Label wlSetParams;
  private Button wSetParams;
  private FormData fdlSetParams;
  private FormData fdSetParams;

  private Label wlSingleStatement;
  private Button wSingleStatement;

  private Label wlInsertField;

  private Text wInsertField;

  private FormData fdlInsertField, fdInsertField;

  private Label wlUpdateField;

  private Text wUpdateField;

  private FormData fdlUpdateField, fdUpdateField;

  private Label wlDeleteField;

  private Text wDeleteField;

  private FormData fdlDeleteField, fdDeleteField;

  private Label wlReadField;

  private Text wReadField;

  private FormData fdlReadField, fdReadField;

  private Label wlFields;

  private TableView wFields;

  private FormData fdlFields, fdFields;

  private Label wlVariables;
  private Button wVariables;
  private FormData fdlVariables, fdVariables;

  private Label wlQuoteString;
  private Button wQuoteString;
  private FormData fdlQuoteString, fdQuoteString;

  private ExecSQLMeta input;
  private boolean changedInDialog;

  private Label wlPosition;
  private FormData fdlPosition;

  private Map<String, Integer> inputFields;

  private ColumnInfo[] colinf;

  public ExecSQLDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (ExecSQLMeta) in;
    inputFields = new HashMap<String, Integer>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        changedInDialog = true;
        input.setChanged();
      }
    };

    SelectionAdapter lsSel = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ExecSQLDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ExecSQLDialog.Stepname.Label" ) );
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

    // Connection line
    wConnection = addConnectionLine( shell, wStepname, input.getDatabaseMeta(), lsMod );

    // Table line...
    wlSQL = new Label( shell, SWT.LEFT );
    wlSQL.setText( BaseMessages.getString( PKG, "ExecSQLDialog.SQL.Label" ) );
    props.setLook( wlSQL );
    fdlSQL = new FormData();
    fdlSQL.left = new FormAttachment( 0, 0 );
    fdlSQL.top = new FormAttachment( wConnection, margin * 2 );
    wlSQL.setLayoutData( fdlSQL );

    wSQL =
      new StyledTextComp( transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wSQL, Props.WIDGET_STYLE_FIXED );
    wSQL.addModifyListener( lsMod );
    wSQL.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        setPosition();
      }

    } );

    wSQL.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        setPosition();
      }

      public void keyReleased( KeyEvent e ) {
        setPosition();
      }
    } );
    wSQL.addFocusListener( new FocusAdapter() {
      public void focusGained( FocusEvent e ) {
        setPosition();
      }

      public void focusLost( FocusEvent e ) {
        setPosition();
      }
    } );
    wSQL.addMouseListener( new MouseAdapter() {
      public void mouseDoubleClick( MouseEvent e ) {
        setPosition();
      }

      public void mouseDown( MouseEvent e ) {
        setPosition();
      }

      public void mouseUp( MouseEvent e ) {
        setPosition();
      }
    } );

    // Text Higlighting
    wSQL.addLineStyleListener( new SQLValuesHighlight() );

    // Some buttons
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "ExecSQLDialog.GetFields.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    setButtonPositions( new Button[] { wOK, wCancel, wGet }, margin, null );

    // Build it up from the bottom up...
    // Read field
    //
    wlReadField = new Label( shell, SWT.RIGHT );
    wlReadField.setText( BaseMessages.getString( PKG, "ExecSQLDialog.ReadField.Label" ) );
    props.setLook( wlReadField );
    fdlReadField = new FormData();
    fdlReadField.left = new FormAttachment( middle, margin );
    fdlReadField.right = new FormAttachment( middle * 2, -margin );
    fdlReadField.bottom = new FormAttachment( wOK, -3 * margin );
    wlReadField.setLayoutData( fdlReadField );
    wReadField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReadField );
    wReadField.addModifyListener( lsMod );
    fdReadField = new FormData();
    fdReadField.left = new FormAttachment( middle * 2, 0 );
    fdReadField.bottom = new FormAttachment( wOK, -3 * margin );
    fdReadField.right = new FormAttachment( 100, 0 );
    wReadField.setLayoutData( fdReadField );

    // Delete field
    //
    wlDeleteField = new Label( shell, SWT.RIGHT );
    wlDeleteField.setText( BaseMessages.getString( PKG, "ExecSQLDialog.DeleteField.Label" ) );
    props.setLook( wlDeleteField );
    fdlDeleteField = new FormData();
    fdlDeleteField.left = new FormAttachment( middle, margin );
    fdlDeleteField.right = new FormAttachment( middle * 2, -margin );
    fdlDeleteField.bottom = new FormAttachment( wReadField, -margin );
    wlDeleteField.setLayoutData( fdlDeleteField );
    wDeleteField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDeleteField );
    wDeleteField.addModifyListener( lsMod );
    fdDeleteField = new FormData();
    fdDeleteField.left = new FormAttachment( middle * 2, 0 );
    fdDeleteField.bottom = new FormAttachment( wReadField, -margin );
    fdDeleteField.right = new FormAttachment( 100, 0 );
    wDeleteField.setLayoutData( fdDeleteField );

    // Update field
    //
    wlUpdateField = new Label( shell, SWT.RIGHT );
    wlUpdateField.setText( BaseMessages.getString( PKG, "ExecSQLDialog.UpdateField.Label" ) );
    props.setLook( wlUpdateField );
    fdlUpdateField = new FormData();
    fdlUpdateField.left = new FormAttachment( middle, margin );
    fdlUpdateField.right = new FormAttachment( middle * 2, -margin );
    fdlUpdateField.bottom = new FormAttachment( wDeleteField, -margin );
    wlUpdateField.setLayoutData( fdlUpdateField );
    wUpdateField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUpdateField );
    wUpdateField.addModifyListener( lsMod );
    fdUpdateField = new FormData();
    fdUpdateField.left = new FormAttachment( middle * 2, 0 );
    fdUpdateField.bottom = new FormAttachment( wDeleteField, -margin );
    fdUpdateField.right = new FormAttachment( 100, 0 );
    wUpdateField.setLayoutData( fdUpdateField );

    // insert field
    //
    wlInsertField = new Label( shell, SWT.RIGHT );
    wlInsertField.setText( BaseMessages.getString( PKG, "ExecSQLDialog.InsertField.Label" ) );
    props.setLook( wlInsertField );
    fdlInsertField = new FormData();
    fdlInsertField.left = new FormAttachment( middle, margin );
    fdlInsertField.right = new FormAttachment( middle * 2, -margin );
    fdlInsertField.bottom = new FormAttachment( wUpdateField, -margin );
    wlInsertField.setLayoutData( fdlInsertField );
    wInsertField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInsertField );
    wInsertField.addModifyListener( lsMod );
    fdInsertField = new FormData();
    fdInsertField.left = new FormAttachment( middle * 2, 0 );
    fdInsertField.bottom = new FormAttachment( wUpdateField, -margin );
    fdInsertField.right = new FormAttachment( 100, 0 );
    wInsertField.setLayoutData( fdInsertField );

    // Setup the "Parameters" label
    //
    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "ExecSQLDialog.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, 0 );
    fdlFields.bottom = new FormAttachment( wInsertField, -25 );
    wlFields.setLayoutData( fdlFields );

    // Parameter fields...
    //
    final int FieldsRows = input.getArguments().length;

    colinf =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "ExecSQLDialog.ColumnInfo.ArgumentFieldname" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ), };

    wFields =
      new TableView(
        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );
    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( middle, 0 );
    fdFields.bottom = new FormAttachment( wOK, -3 * margin );
    wFields.setLayoutData( fdFields );

    // For the "execute for each row" and "variable substitution" labels,
    // find their maximum width
    // and use that in the alignment
    //
    wlEachRow = new Label( shell, SWT.RIGHT );
    wlEachRow.setText( BaseMessages.getString( PKG, "ExecSQLDialog.EachRow.Label" ) );
    wlEachRow.pack();
    wlSingleStatement = new Label( shell, SWT.RIGHT );
    wlSingleStatement.setText( BaseMessages.getString( PKG, "ExecSQLDialog.SingleStatement.Label" ) );
    wlSingleStatement.pack();
    wlVariables = new Label( shell, SWT.RIGHT );
    wlVariables.setText( BaseMessages.getString( PKG, "ExecSQLDialog.ReplaceVariables" ) );
    wlVariables.pack();
    wlQuoteString = new Label( shell, SWT.RIGHT );
    wlQuoteString.setText( BaseMessages.getString( PKG, "ExecSQLDialog.QuoteString.Label" ) );
    wlQuoteString.pack();
    Rectangle rEachRow = wlEachRow.getBounds();
    Rectangle rSingleStatement = wlSingleStatement.getBounds();
    Rectangle rVariables = wlVariables.getBounds();
    Rectangle rQuoteString = wlQuoteString.getBounds();
    int width =
      Math.max(
        Math.max( Math.max( rEachRow.width, rSingleStatement.width ), rVariables.width ), rQuoteString.width ) + 30;

    // Setup the "Quote String" label and checkbox
    //
    props.setLook( wlQuoteString );
    fdlQuoteString = new FormData();
    fdlQuoteString.left = new FormAttachment( 0, margin );
    fdlQuoteString.right = new FormAttachment( 0, width );
    fdlQuoteString.bottom = new FormAttachment( wlFields, -2 * margin );
    wlQuoteString.setLayoutData( fdlQuoteString );
    wQuoteString = new Button( shell, SWT.CHECK );
    props.setLook( wQuoteString );
    wQuoteString.setToolTipText( BaseMessages.getString( PKG, "ExecSQLDialog.QuoteString.Tooltip" ) );
    fdQuoteString = new FormData();
    fdQuoteString.left = new FormAttachment( wlQuoteString, margin );
    fdQuoteString.bottom = new FormAttachment( wlFields, -2 * margin );
    fdQuoteString.right = new FormAttachment( middle, 0 );
    wQuoteString.setLayoutData( fdQuoteString );
    wQuoteString.addSelectionListener( lsSel );

    // Setup the "Bind parameters" label and checkbox
    //
    wlSetParams = new Label( this.shell, SWT.RIGHT );
    wlSetParams.setText( BaseMessages.getString( PKG, "ExecSQLDialog.SetParams.Label" ) );
    props.setLook( this.wlSetParams );
    fdlSetParams = new FormData();
    fdlSetParams.left = new FormAttachment( 0, margin );
    fdlSetParams.bottom = new FormAttachment( wQuoteString, -margin );
    fdlSetParams.right = new FormAttachment( 0, width );
    wlSetParams.setLayoutData( this.fdlSetParams );
    wSetParams = new Button( shell, SWT.CHECK );
    props.setLook( this.wSetParams );
    wSetParams.setToolTipText( BaseMessages.getString( PKG, "ExecSQLDialog.SetParams.Tooltip" ) );
    fdSetParams = new FormData();
    fdSetParams.left = new FormAttachment( wlSetParams, margin );
    fdSetParams.bottom = new FormAttachment( wQuoteString, -margin );
    fdSetParams.right = new FormAttachment( middle, 0 );
    wSetParams.setLayoutData( fdSetParams );
    wSetParams.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setExecutedSetParams();
        input.setChanged();
      }
    } );

    // Setup the "variable substitution" label and checkbox
    //
    props.setLook( wlVariables );
    fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment( 0, margin );
    fdlVariables.right = new FormAttachment( 0, width );
    fdlVariables.bottom = new FormAttachment( wSetParams, -margin );
    wlVariables.setLayoutData( fdlVariables );
    wVariables = new Button( shell, SWT.CHECK );
    props.setLook( wVariables );
    fdVariables = new FormData();
    fdVariables.left = new FormAttachment( wlVariables, margin );
    fdVariables.bottom = new FormAttachment( wSetParams, -margin );
    fdVariables.right = new FormAttachment( middle, 0 );
    wVariables.setLayoutData( fdVariables );
    wVariables.addSelectionListener( lsSel );

    // Setup the "Single statement" label and checkbox
    //
    props.setLook( wlSingleStatement );
    FormData fdlSingleStatement = new FormData();
    fdlSingleStatement.left = new FormAttachment( 0, margin );
    fdlSingleStatement.right = new FormAttachment( 0, width );
    fdlSingleStatement.bottom = new FormAttachment( wVariables, -margin );
    wlSingleStatement.setLayoutData( fdlSingleStatement );
    wSingleStatement = new Button( shell, SWT.CHECK );
    props.setLook( wSingleStatement );
    FormData fdSingleStatement = new FormData();
    fdSingleStatement.left = new FormAttachment( wlEachRow, margin );
    fdSingleStatement.bottom = new FormAttachment( wVariables, -margin );
    fdSingleStatement.right = new FormAttachment( middle, 0 );
    wSingleStatement.setLayoutData( fdSingleStatement );
    wSingleStatement.addSelectionListener( lsSel );

    // Setup the "execute for each row" label and checkbox
    //
    props.setLook( wlEachRow );
    FormData fdlEachRow = new FormData();
    fdlEachRow.left = new FormAttachment( 0, margin );
    fdlEachRow.right = new FormAttachment( 0, width );
    fdlEachRow.bottom = new FormAttachment( wSingleStatement, -margin );
    wlEachRow.setLayoutData( fdlEachRow );
    wEachRow = new Button( shell, SWT.CHECK );
    props.setLook( wEachRow );
    FormData fdEachRow = new FormData();
    fdEachRow.left = new FormAttachment( wlEachRow, margin );
    fdEachRow.bottom = new FormAttachment( wSingleStatement, -margin );
    fdEachRow.right = new FormAttachment( middle, 0 );
    wEachRow.setLayoutData( fdEachRow );
    wEachRow.addSelectionListener( lsSel );

    // Position label under the SQL editor
    //
    wlPosition = new Label( shell, SWT.NONE );
    props.setLook( wlPosition );
    fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( 0, 0 );
    fdlPosition.right = new FormAttachment( 100, 0 );
    fdlPosition.bottom = new FormAttachment( wEachRow, -2 * margin ); // 2 times since we deal with bottom instead of
    // top
    wlPosition.setLayoutData( fdlPosition );

    // Finally, the SQL editor takes up all other space between the position and the SQL label
    //
    fdSQL = new FormData();
    fdSQL.left = new FormAttachment( 0, 0 );
    fdSQL.top = new FormAttachment( wlSQL, margin );
    fdSQL.right = new FormAttachment( 100, -2 * margin );
    fdSQL.bottom = new FormAttachment( wlPosition, -margin );
    wSQL.setLayoutData( fdSQL );

    // Search the fields in the background
    //
    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }
            setComboBoxes();
          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    // Add listeners
    //
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
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wGet.addListener( SWT.Selection, lsGet );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    wEachRow.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        ExecSQLDialog.this.setExecutedEachInputRow();
        ExecSQLDialog.this.input.setChanged();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        checkCancel( e );
      }
    } );

    getData();
    setExecutedEachInputRow();
    setExecutedSetParams();
    changedInDialog = false; // for prompting if dialog is simply closed
    input.setChanged( changed );

    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void setExecutedEachInputRow() {
    wlFields.setEnabled( wEachRow.getSelection() );
    wFields.setEnabled( wEachRow.getSelection() );
    wlSetParams.setEnabled( wEachRow.getSelection() );
    wSetParams.setEnabled( wEachRow.getSelection() );
    if ( !wEachRow.getSelection() ) {
      wSetParams.setSelection( wEachRow.getSelection() );
    }
    wlQuoteString.setEnabled( wEachRow.getSelection() );
    wQuoteString.setEnabled( wEachRow.getSelection() );
    if ( !wEachRow.getSelection() ) {
      wQuoteString.setSelection( wEachRow.getSelection() );
    }

  }

  private void setExecutedSetParams() {
    wlQuoteString.setEnabled( !wSetParams.getSelection() );
    wQuoteString.setEnabled( !wSetParams.getSelection() );
    if ( wSetParams.getSelection() ) {
      wQuoteString.setSelection( !wSetParams.getSelection() );
    }

  }

  public void setPosition() {

    String scr = wSQL.getText();
    int linenr = wSQL.getLineAtOffset( wSQL.getCaretOffset() ) + 1;
    int posnr = wSQL.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while ( posnr > 0 && scr.charAt( posnr - 1 ) != '\n' && scr.charAt( posnr - 1 ) != '\r' ) {
      posnr--;
      colnr++;
    }
    wlPosition.setText( BaseMessages.getString( PKG, "ExecSQLDialog.Position.Label", "" + linenr, "" + colnr ) );

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

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getSql() != null ) {
      wSQL.setText( input.getSql() );
    }
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }
    wEachRow.setSelection( input.isExecutedEachInputRow() );
    wSingleStatement.setSelection( input.isSingleStatement() );
    wVariables.setSelection( input.isReplaceVariables() );
    wQuoteString.setSelection( input.isQuoteString() );

    if ( input.getUpdateField() != null ) {
      wUpdateField.setText( input.getUpdateField() );
    }
    if ( input.getInsertField() != null ) {
      wInsertField.setText( input.getInsertField() );
    }
    if ( input.getDeleteField() != null ) {
      wDeleteField.setText( input.getDeleteField() );
    }
    if ( input.getReadField() != null ) {
      wReadField.setText( input.getReadField() );
    }

    for ( int i = 0; i < input.getArguments().length; i++ ) {
      TableItem item = wFields.table.getItem( i );
      if ( input.getArguments()[ i ] != null ) {
        item.setText( 1, input.getArguments()[ i ] );
      }
    }
    wSetParams.setSelection( input.isParams() );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void checkCancel( ShellEvent e ) {
    if ( changedInDialog ) {
      int save = JobGraph.showChangedWarning( shell, wStepname.getText() );
      if ( save == SWT.CANCEL ) {
        e.doit = false;
      } else if ( save == SWT.YES ) {
        ok();
      } else {
        cancel();
      }
    } else {
      cancel();
    }
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    // copy info to TextFileInputMeta class (input)
    input.setSql( wSQL.getText() );
    input.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
    input.setExecutedEachInputRow( wEachRow.getSelection() );
    input.setSingleStatement( wSingleStatement.getSelection() );
    input.setVariableReplacementActive( wVariables.getSelection() );
    input.setQuoteString( wQuoteString.getSelection() );
    input.setParams( wSetParams.getSelection() );
    input.setInsertField( wInsertField.getText() );
    input.setUpdateField( wUpdateField.getText() );
    input.setDeleteField( wDeleteField.getText() );
    input.setReadField( wReadField.getText() );

    int nrargs = wFields.nrNonEmpty();
    input.allocate( nrargs );
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "ExecSQLDialog.Log.FoundArguments", +nrargs + "" ) );
    }
    for ( int i = 0; i < nrargs; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      //CHECKSTYLE:Indentation:OFF
      input.getArguments()[ i ] = item.getText( 1 );
    }

    if ( input.getDatabaseMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ExecSQLDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "ExecSQLDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        BaseStepDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "ExecSQLDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "ExecSQLDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }
}
