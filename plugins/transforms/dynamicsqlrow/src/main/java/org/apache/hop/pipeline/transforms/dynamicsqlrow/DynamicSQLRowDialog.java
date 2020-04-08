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

package org.apache.hop.pipeline.transforms.dynamicsqlrow;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transforms.tableinput.SQLValuesHighlight;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class DynamicSQLRowDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = DynamicSQLRowMeta.class; // for i18n purposes, needed by Translator!!

  private boolean gotPreviousFields = false;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlSQL;
  private StyledTextComp wSql;
  private FormData fdlSQL, fdSQL;

  private Label wlLimit;
  private Text wLimit;
  private FormData fdlLimit, fdLimit;

  private Label wlOuter;
  private Button wOuter;
  private FormData fdlOuter, fdOuter;

  private Label wluseVars;
  private Button wuseVars;
  private FormData fdluseVars, fduseVars;

  private Label wlPosition;
  private FormData fdlPosition;

  private Label wlSQLFieldName;
  private CCombo wSqlFieldName;
  private FormData fdlSQLFieldName, fdSQLFieldName;

  private Label wlqueryOnlyOnChange;
  private Button wqueryOnlyOnChange;
  private FormData fdlqueryOnlyOnChange, fdqueryOnlyOnChange;

  private DynamicSQLRowMeta input;

  public DynamicSQLRowDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    input = (DynamicSQLRowMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.TransformName.Label" ) );
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

    // Connection line
    wConnection = addConnectionLine( shell, wTransformName, input.getDatabaseMeta(), lsMod );
    if ( input.getDatabaseMeta() == null && pipelineMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );

    // SQLFieldName field
    wlSQLFieldName = new Label( shell, SWT.RIGHT );
    wlSQLFieldName.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.SQLFieldName.Label" ) );
    props.setLook( wlSQLFieldName );
    fdlSQLFieldName = new FormData();
    fdlSQLFieldName.left = new FormAttachment( 0, 0 );
    fdlSQLFieldName.right = new FormAttachment( middle, -margin );
    fdlSQLFieldName.top = new FormAttachment( wConnection, 2 * margin );
    wlSQLFieldName.setLayoutData( fdlSQLFieldName );
    wSqlFieldName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wSqlFieldName.setEditable( true );
    props.setLook( wSqlFieldName );
    wSqlFieldName.addModifyListener( lsMod );
    fdSQLFieldName = new FormData();
    fdSQLFieldName.left = new FormAttachment( middle, 0 );
    fdSQLFieldName.top = new FormAttachment( wConnection, 2 * margin );
    fdSQLFieldName.right = new FormAttachment( 100, -margin );
    wSqlFieldName.setLayoutData( fdSQLFieldName );
    wSqlFieldName.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // Limit the number of lines returns
    wlLimit = new Label( shell, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.right = new FormAttachment( middle, -margin );
    fdlLimit.top = new FormAttachment( wSqlFieldName, margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.right = new FormAttachment( 100, 0 );
    fdLimit.top = new FormAttachment( wSqlFieldName, margin );
    wLimit.setLayoutData( fdLimit );

    // Outer join?
    wlOuter = new Label( shell, SWT.RIGHT );
    wlOuter.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.Outerjoin.Label" ) );
    wlOuter.setToolTipText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.Outerjoin.Tooltip" ) );
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
    wluseVars.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.useVarsjoin.Label" ) );
    wluseVars.setToolTipText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.useVarsjoin.Tooltip" ) );
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

    // queryOnlyOnChange ?
    wlqueryOnlyOnChange = new Label( shell, SWT.RIGHT );
    wlqueryOnlyOnChange.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.queryOnlyOnChangejoin.Label" ) );
    wlqueryOnlyOnChange.setToolTipText( BaseMessages.getString(
      PKG, "DynamicSQLRowDialog.queryOnlyOnChangejoin.Tooltip" ) );
    props.setLook( wlqueryOnlyOnChange );
    fdlqueryOnlyOnChange = new FormData();
    fdlqueryOnlyOnChange.left = new FormAttachment( 0, 0 );
    fdlqueryOnlyOnChange.right = new FormAttachment( middle, -margin );
    fdlqueryOnlyOnChange.top = new FormAttachment( wuseVars, margin );
    wlqueryOnlyOnChange.setLayoutData( fdlqueryOnlyOnChange );
    wqueryOnlyOnChange = new Button( shell, SWT.CHECK );
    props.setLook( wqueryOnlyOnChange );
    wqueryOnlyOnChange.setToolTipText( wlqueryOnlyOnChange.getToolTipText() );
    fdqueryOnlyOnChange = new FormData();
    fdqueryOnlyOnChange.left = new FormAttachment( middle, 0 );
    fdqueryOnlyOnChange.top = new FormAttachment( wuseVars, margin );
    wqueryOnlyOnChange.setLayoutData( fdqueryOnlyOnChange );
    wqueryOnlyOnChange.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // SQL editor...
    wlSQL = new Label( shell, SWT.NONE );
    wlSQL.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.SQL.Label" ) );
    props.setLook( wlSQL );
    fdlSQL = new FormData();
    fdlSQL.left = new FormAttachment( 0, 0 );
    fdlSQL.top = new FormAttachment( wqueryOnlyOnChange, margin );
    wlSQL.setLayoutData( fdlSQL );

    wSql =
      new StyledTextComp( pipelineMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wSql, Props.WIDGET_STYLE_FIXED );
    fdSQL = new FormData();
    fdSQL.left = new FormAttachment( 0, 0 );
    fdSQL.top = new FormAttachment( wlSQL, margin );
    fdSQL.right = new FormAttachment( 100, -2 * margin );
    fdSQL.bottom = new FormAttachment( wOk, -4 * margin );
    wSql.setLayoutData( fdSQL );

    wSql.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        setPosition();
      }

    } );

    wSql.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        setPosition();
      }

      public void keyReleased( KeyEvent e ) {
        setPosition();
      }
    } );
    wSql.addFocusListener( new FocusAdapter() {
      public void focusGained( FocusEvent e ) {
        setPosition();
      }

      public void focusLost( FocusEvent e ) {
        setPosition();
      }
    } );
    wSql.addMouseListener( new MouseAdapter() {
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

    wSql.addModifyListener( lsMod );

    // Text Higlighting
    wSql.addLineStyleListener( new SQLValuesHighlight() );

    wlPosition = new Label( shell, SWT.NONE );
    // wlPosition.setText(BaseMessages.getString(PKG, "DynamicSQLRowDialog.Position.Label"));
    props.setLook( wlPosition );
    fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( 0, 0 );
    fdlPosition.top = new FormAttachment( wSql, margin );
    fdlPosition.right = new FormAttachment( 100, 0 );
    wlPosition.setLayoutData( fdlPosition );

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
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
    return transformName;
  }

  public void setPosition() {

    String scr = wSql.getText();
    int linenr = wSql.getLineAtOffset( wSql.getCaretOffset() ) + 1;
    int posnr = wSql.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while ( posnr > 0 && scr.charAt( posnr - 1 ) != '\n' && scr.charAt( posnr - 1 ) != '\r' ) {
      posnr--;
      colnr++;
    }

    wlPosition.setText( BaseMessages
      .getString( PKG, "DynamicSQLRowDialog.Position.Label", "" + linenr, "" + colnr ) );

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "DynamicSQLRowDialog.Log.GettingKeyInfo" ) );
    }

    wSql.setText( Const.NVL( input.getSql(), "" ) );
    wLimit.setText( "" + input.getRowLimit() );
    wOuter.setSelection( input.isOuterJoin() );
    wuseVars.setSelection( input.isVariableReplace() );
    if ( input.getSqlFieldName() != null ) {
      wSqlFieldName.setText( input.getSqlFieldName() );
    }
    wqueryOnlyOnChange.setSelection( input.isQueryOnlyOnChange() );
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    input.setRowLimit( Const.toInt( wLimit.getText(), 0 ) );
    input.setSql( wSql.getText() );
    input.setSqlFieldName( wSqlFieldName.getText() );
    input.setOuterJoin( wOuter.getSelection() );
    input.setVariableReplace( wuseVars.getSelection() );
    input.setQueryOnlyOnChange( wqueryOnlyOnChange.getSelection() );
    input.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );

    transformName = wTransformName.getText(); // return value

    if ( pipelineMeta.findDatabase( wConnection.getText() ) == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "DynamicSQLRowDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "DynamicSQLRowDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      gotPreviousFields = true;
      try {
        String sqlfield = wSqlFieldName.getText();
        wSqlFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
        if ( r != null ) {
          wSqlFieldName.removeAll();
          wSqlFieldName.setItems( r.getFieldNames() );
        }
        if ( sqlfield != null ) {
          wSqlFieldName.setText( sqlfield );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "DynamicSQLRowDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "DynamicSQLRowDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }
}
