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

package org.apache.hop.workflow.actions.sql;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transforms.tableinput.SQLValuesHighlight;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the SQL action settings. (select the connection and the sql script to be executed)
 *
 * @author Matt
 * @since 19-06-2003
 */
@PluginDialog( 
		  id = "SQL", 
		  image = "SQL.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class ActionSQLDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionSQL.class; // for i18n purposes, needed by Translator!!

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobSQL.Filetype.Sql" ), BaseMessages.getString( PKG, "JobSQL.Filetype.Text" ),
    BaseMessages.getString( PKG, "JobSQL.Filetype.All" ) };

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlUseSubs;

  private Button wUseSubs;

  private Button wSqlFromFile;

  private Label wlSQLFromFile;

  private FormData fdlUseSubs, fdUseSubs;

  private FormData fdlSQLFromFile, fdSQLFromFile;

  private Label wlSQL;

  private StyledTextComp wSql;

  private FormData fdlSQL, fdSQL;

  private Label wlPosition;

  private FormData fdlPosition;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionSQL jobEntry;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Label wlUseOneStatement;

  private Button wSendOneStatement;

  private FormData fdlUseOneStatement, fdUseOneStatement;

  // File
  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;
  private FormData fdlFilename, fdbFilename, fdFilename;

  public ActionSQLDialog( Shell parent, IAction jobEntryInt, WorkflowMeta workflowMeta ) {
    super( parent, jobEntryInt, workflowMeta );
    jobEntry = (ActionSQL) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobSQL.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getWorkflowsDialogStyle() );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobSQL.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, null );

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobSQL.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    // Connection line
    wConnection = addConnectionLine( shell, wName, jobEntry.getDatabase(), lsMod );

    // SQL from file?
    wlSQLFromFile = new Label( shell, SWT.RIGHT );
    wlSQLFromFile.setText( BaseMessages.getString( PKG, "JobSQL.SQLFromFile.Label" ) );
    props.setLook( wlSQLFromFile );
    fdlSQLFromFile = new FormData();
    fdlSQLFromFile.left = new FormAttachment( 0, 0 );
    fdlSQLFromFile.top = new FormAttachment( wConnection, 2 * margin );
    fdlSQLFromFile.right = new FormAttachment( middle, -margin );
    wlSQLFromFile.setLayoutData( fdlSQLFromFile );
    wSqlFromFile = new Button( shell, SWT.CHECK );
    props.setLook( wSqlFromFile );
    wSqlFromFile.setToolTipText( BaseMessages.getString( PKG, "JobSQL.SQLFromFile.Tooltip" ) );
    fdSQLFromFile = new FormData();
    fdSQLFromFile.left = new FormAttachment( middle, 0 );
    fdSQLFromFile.top = new FormAttachment( wConnection, 2 * margin );
    fdSQLFromFile.right = new FormAttachment( 100, 0 );
    wSqlFromFile.setLayoutData( fdSQLFromFile );
    wSqlFromFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeSQLFromFile();
        jobEntry.setChanged();
      }
    } );

    // Filename line
    wlFilename = new Label( shell, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JobSQL.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wSqlFromFile, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wSqlFromFile, margin );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.setToolTipText( BaseMessages.getString( PKG, "JobSQL.Filename.Tooltip" ) );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wSqlFromFile, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wFilename.setToolTipText( workflowMeta.environmentSubstitute( wFilename.getText() ) );
      }
    } );

    wbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.sql", "*.txt", "*" } );
        if ( wFilename.getText() != null ) {
          dialog.setFileName( workflowMeta.environmentSubstitute( wFilename.getText() ) );
        }
        dialog.setFilterNames( FILETYPES );
        if ( dialog.open() != null ) {
          wFilename.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    // Send one SQL Statement?
    wlUseOneStatement = new Label( shell, SWT.RIGHT );
    wlUseOneStatement.setText( BaseMessages.getString( PKG, "JobSQL.SendOneStatement.Label" ) );
    props.setLook( wlUseOneStatement );
    fdlUseOneStatement = new FormData();
    fdlUseOneStatement.left = new FormAttachment( 0, 0 );
    fdlUseOneStatement.top = new FormAttachment( wbFilename, margin );
    fdlUseOneStatement.right = new FormAttachment( middle, -margin );
    wlUseOneStatement.setLayoutData( fdlUseOneStatement );
    wSendOneStatement = new Button( shell, SWT.CHECK );
    props.setLook( wSendOneStatement );
    wSendOneStatement.setToolTipText( BaseMessages.getString( PKG, "JobSQL.SendOneStatement.Tooltip" ) );
    fdUseOneStatement = new FormData();
    fdUseOneStatement.left = new FormAttachment( middle, 0 );
    fdUseOneStatement.top = new FormAttachment( wbFilename, margin );
    fdUseOneStatement.right = new FormAttachment( 100, 0 );
    wSendOneStatement.setLayoutData( fdUseOneStatement );
    wSendOneStatement.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        jobEntry.setChanged();
      }
    } );

    // Use variable substitution?
    wlUseSubs = new Label( shell, SWT.RIGHT );
    wlUseSubs.setText( BaseMessages.getString( PKG, "JobSQL.UseVariableSubst.Label" ) );
    props.setLook( wlUseSubs );
    fdlUseSubs = new FormData();
    fdlUseSubs.left = new FormAttachment( 0, 0 );
    fdlUseSubs.top = new FormAttachment( wSendOneStatement, margin );
    fdlUseSubs.right = new FormAttachment( middle, -margin );
    wlUseSubs.setLayoutData( fdlUseSubs );
    wUseSubs = new Button( shell, SWT.CHECK );
    props.setLook( wUseSubs );
    wUseSubs.setToolTipText( BaseMessages.getString( PKG, "JobSQL.UseVariableSubst.Tooltip" ) );
    fdUseSubs = new FormData();
    fdUseSubs.left = new FormAttachment( middle, 0 );
    fdUseSubs.top = new FormAttachment( wSendOneStatement, margin );
    fdUseSubs.right = new FormAttachment( 100, 0 );
    wUseSubs.setLayoutData( fdUseSubs );
    wUseSubs.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        jobEntry.setUseVariableSubstitution( !jobEntry.getUseVariableSubstitution() );
        jobEntry.setChanged();
      }
    } );

    wlPosition = new Label( shell, SWT.NONE );
    wlPosition.setText( BaseMessages.getString( PKG, "JobSQL.LineNr.Label", "0" ) );
    props.setLook( wlPosition );
    fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( 0, 0 );
    fdlPosition.right = new FormAttachment( 100, 0 );
    fdlPosition.bottom = new FormAttachment( wOk, -margin );
    wlPosition.setLayoutData( fdlPosition );

    // Script line
    wlSQL = new Label( shell, SWT.NONE );
    wlSQL.setText( BaseMessages.getString( PKG, "JobSQL.Script.Label" ) );
    props.setLook( wlSQL );
    fdlSQL = new FormData();
    fdlSQL.left = new FormAttachment( 0, 0 );
    fdlSQL.top = new FormAttachment( wUseSubs, margin );
    wlSQL.setLayoutData( fdlSQL );

    wSql =
      new StyledTextComp( jobEntry, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wSql, Props.WIDGET_STYLE_FIXED );
    wSql.addModifyListener( lsMod );
    fdSQL = new FormData();
    fdSQL.left = new FormAttachment( 0, 0 );
    fdSQL.top = new FormAttachment( wlSQL, margin );
    fdSQL.right = new FormAttachment( 100, -10 );
    fdSQL.bottom = new FormAttachment( wlPosition, -margin );
    wSql.setLayoutData( fdSQL );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

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

    getData();
    activeSQLFromFile();

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobSQLDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
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
    wlPosition.setText( BaseMessages.getString( PKG, "JobSQL.Position.Label", "" + linenr, "" + colnr ) );

  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( jobEntry.getName() ) );
    wSql.setText( Const.nullToEmpty( jobEntry.getSql() ) );
    DatabaseMeta dbinfo = jobEntry.getDatabase();
    if ( dbinfo != null && dbinfo.getName() != null ) {
      wConnection.setText( dbinfo.getName() );
    } else {
      wConnection.setText( "" );
    }

    wUseSubs.setSelection( jobEntry.getUseVariableSubstitution() );
    wSqlFromFile.setSelection( jobEntry.getSqlFromFile() );
    wSendOneStatement.setSelection( jobEntry.isSendOneStatement() );

    wFilename.setText( Const.nullToEmpty( jobEntry.getSqlFilename() ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void activeSQLFromFile() {
    wlFilename.setEnabled( wSqlFromFile.getSelection() );
    wFilename.setEnabled( wSqlFromFile.getSelection() );
    wbFilename.setEnabled( wSqlFromFile.getSelection() );
    wSql.setEnabled( !wSqlFromFile.getSelection() );
    wlSQL.setEnabled( !wSqlFromFile.getSelection() );
    wlPosition.setEnabled( !wSqlFromFile.getSelection() );

  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setSql( wSql.getText() );
    jobEntry.setUseVariableSubstitution( wUseSubs.getSelection() );
    jobEntry.setSqlFromFile( wSqlFromFile.getSelection() );
    jobEntry.setSqlFilename( wFilename.getText() );
    jobEntry.setSendOneStatement( wSendOneStatement.getSelection() );
    jobEntry.setDatabase( workflowMeta.findDatabase( wConnection.getText() ) );
    dispose();
  }
}
