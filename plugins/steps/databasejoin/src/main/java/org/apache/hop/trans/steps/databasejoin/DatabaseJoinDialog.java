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

package org.apache.hop.trans.steps.databasejoin;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
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
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.apache.hop.ui.trans.steps.tableinput.SQLValuesHighlight;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class DatabaseJoinDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = DatabaseJoinMeta.class; // for i18n purposes, needed by Translator2!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlSQL;
  private StyledTextComp wSQL;
  private FormData fdlSQL, fdSQL;

  private Label wlLimit;
  private Text wLimit;
  private FormData fdlLimit, fdLimit;

  private Label wlOuter;
  private Button wOuter;
  private FormData fdlOuter, fdOuter;

  private Label wlParam;
  private TableView wParam;
  private FormData fdlParam, fdParam;

  private Label wluseVars;
  private Button wuseVars;
  private FormData fdluseVars, fduseVars;

  private Button wGet;
  private Listener lsGet;

  private DatabaseJoinMeta input;

  private Label wlPosition;
  private FormData fdlPosition;

  private ColumnInfo[] ciKey;

  private Map<String, Integer> inputFields;

  public DatabaseJoinDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (DatabaseJoinMeta) in;
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
        input.setChanged();
      }
    };
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.Stepname.Label" ) );
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

    // SQL editor...
    wlSQL = new Label( shell, SWT.NONE );
    wlSQL.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.SQL.Label" ) );
    props.setLook( wlSQL );
    fdlSQL = new FormData();
    fdlSQL.left = new FormAttachment( 0, 0 );
    fdlSQL.top = new FormAttachment( wConnection, margin * 2 );
    wlSQL.setLayoutData( fdlSQL );

    wSQL =
      new StyledTextComp( transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wSQL, Props.WIDGET_STYLE_FIXED );
    wSQL.addModifyListener( lsMod );
    fdSQL = new FormData();
    fdSQL.left = new FormAttachment( 0, 0 );
    fdSQL.top = new FormAttachment( wlSQL, margin );
    fdSQL.right = new FormAttachment( 100, -2 * margin );
    fdSQL.bottom = new FormAttachment( 60, 0 );
    wSQL.setLayoutData( fdSQL );

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

    // SQL Higlighting
    wSQL.addLineStyleListener( new SQLValuesHighlight() );

    wlPosition = new Label( shell, SWT.NONE );
    props.setLook( wlPosition );
    fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( 0, 0 );
    fdlPosition.top = new FormAttachment( wSQL, margin );
    fdlPosition.right = new FormAttachment( 100, 0 );
    wlPosition.setLayoutData( fdlPosition );

    // Limit the number of lines returns
    wlLimit = new Label( shell, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.right = new FormAttachment( middle, -margin );
    fdlLimit.top = new FormAttachment( wlPosition, margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.right = new FormAttachment( 100, 0 );
    fdLimit.top = new FormAttachment( wlPosition, margin );
    wLimit.setLayoutData( fdLimit );

    // Outer join?
    wlOuter = new Label( shell, SWT.RIGHT );
    wlOuter.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.Outerjoin.Label" ) );
    wlOuter.setToolTipText( BaseMessages.getString( PKG, "DatabaseJoinDialog.Outerjoin.Tooltip" ) );
    props.setLook( wlOuter );
    fdlOuter = new FormData();
    fdlOuter.left = new FormAttachment( 0, 0 );
    fdlOuter.right = new FormAttachment( middle, -margin );
    fdlOuter.top = new FormAttachment( wLimit, margin );
    wlOuter.setLayoutData( fdlOuter );
    wOuter = new Button( shell, SWT.CHECK );
    props.setLook( wOuter );
    wOuter.setToolTipText( wlOuter.getToolTipText() );
    fdOuter = new FormData();
    fdOuter.left = new FormAttachment( middle, 0 );
    fdOuter.top = new FormAttachment( wLimit, margin );
    wOuter.setLayoutData( fdOuter );
    wOuter.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // useVars ?
    wluseVars = new Label( shell, SWT.RIGHT );
    wluseVars.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.useVarsjoin.Label" ) );
    wluseVars.setToolTipText( BaseMessages.getString( PKG, "DatabaseJoinDialog.useVarsjoin.Tooltip" ) );
    props.setLook( wluseVars );
    fdluseVars = new FormData();
    fdluseVars.left = new FormAttachment( 0, 0 );
    fdluseVars.right = new FormAttachment( middle, -margin );
    fdluseVars.top = new FormAttachment( wOuter, margin );
    wluseVars.setLayoutData( fdluseVars );
    wuseVars = new Button( shell, SWT.CHECK );
    props.setLook( wuseVars );
    wuseVars.setToolTipText( wluseVars.getToolTipText() );
    fduseVars = new FormData();
    fduseVars.left = new FormAttachment( middle, 0 );
    fduseVars.top = new FormAttachment( wOuter, margin );
    wuseVars.setLayoutData( fduseVars );
    wuseVars.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.GetFields.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel, wGet }, margin, null );

    // The parameters
    wlParam = new Label( shell, SWT.NONE );
    wlParam.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.Param.Label" ) );
    props.setLook( wlParam );
    fdlParam = new FormData();
    fdlParam.left = new FormAttachment( 0, 0 );
    fdlParam.top = new FormAttachment( wuseVars, margin );
    wlParam.setLayoutData( fdlParam );

    int nrKeyCols = 2;
    int nrKeyRows = ( input.getParameterField() != null ? input.getParameterField().length : 1 );

    ciKey = new ColumnInfo[ nrKeyCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "DatabaseJoinDialog.ColumnInfo.ParameterFieldname" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciKey[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "DatabaseJoinDialog.ColumnInfo.ParameterType" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() );

    wParam =
      new TableView(
        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
        nrKeyRows, lsMod, props );

    fdParam = new FormData();
    fdParam.left = new FormAttachment( 0, 0 );
    fdParam.top = new FormAttachment( wlParam, margin );
    fdParam.right = new FormAttachment( 100, 0 );
    fdParam.bottom = new FormAttachment( wOK, -2 * margin );
    wParam.setLayoutData( fdParam );

    //
    // Search the fields in the background

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
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wLimit.addSelectionListener( lsDef );

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
    ciKey[ 0 ].setComboValues( fieldNames );
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

    wlPosition
      .setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.Position.Label", "" + linenr, "" + colnr ) );

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    logDebug( BaseMessages.getString( PKG, "DatabaseJoinDialog.Log.GettingKeyInfo" ) );

    wSQL.setText( Const.NVL( input.getSql(), "" ) );
    wLimit.setText( "" + input.getRowLimit() );
    wOuter.setSelection( input.isOuterJoin() );
    wuseVars.setSelection( input.isVariableReplace() );
    if ( input.getParameterField() != null ) {
      for ( int i = 0; i < input.getParameterField().length; i++ ) {
        TableItem item = wParam.table.getItem( i );
        if ( input.getParameterField()[ i ] != null ) {
          item.setText( 1, input.getParameterField()[ i ] );
        }
        if ( input.getParameterType()[ i ] != 0 ) {
          item.setText( 2, ValueMetaFactory.getValueMetaName( input.getParameterType()[ i ] ) );
        }
      }
    }

    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    wParam.setRowNums();
    wParam.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    int nrparam = wParam.nrNonEmpty();

    input.allocate( nrparam );

    input.setRowLimit( Const.toInt( wLimit.getText(), 0 ) );
    input.setSql( wSQL.getText() );

    input.setOuterJoin( wOuter.getSelection() );
    input.setVariableReplace( wuseVars.getSelection() );
    logDebug( BaseMessages.getString( PKG, "DatabaseJoinDialog.Log.ParametersFound" ) + nrparam + " parameters" );
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrparam; i++ ) {
      TableItem item = wParam.getNonEmpty( i );
      input.getParameterField()[ i ] = item.getText( 1 );
      input.getParameterType()[ i ] = ValueMetaFactory.getIdForValueMeta( item.getText( 2 ) );
    }

    input.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );

    stepname = wStepname.getText(); // return value

    if ( transMeta.findDatabase( wConnection.getText() ) == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "DatabaseJoinDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "DatabaseJoinDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        BaseStepDialog.getFieldsFromPrevious( r, wParam, 1, new int[] { 1 }, new int[] { 2 }, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogTitle" ), BaseMessages
        .getString( PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogMessage" ), ke );
    }

  }
}
