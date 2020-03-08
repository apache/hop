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

package org.apache.hop.job.entries.sql;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.sql.JobEntrySQL;
import org.apache.hop.job.entry.JobEntryDialogInterface;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.job.dialog.JobDialog;
import org.apache.hop.ui.job.entry.JobEntryDialog;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.apache.hop.ui.trans.steps.tableinput.SQLValuesHighlight;
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
 * This dialog allows you to edit the SQL job entry settings. (select the connection and the sql script to be executed)
 *
 * @author Matt
 * @since 19-06-2003
 */
@PluginDialog( 
		  id = "SQL", 
		  image = "SQL.svg", 
		  pluginType = PluginDialog.PluginType.JOBENTRY,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class JobEntrySQLDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntrySQL.class; // for i18n purposes, needed by Translator2!!

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobSQL.Filetype.Sql" ), BaseMessages.getString( PKG, "JobSQL.Filetype.Text" ),
    BaseMessages.getString( PKG, "JobSQL.Filetype.All" ) };

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlUseSubs;

  private Button wUseSubs;

  private Button wSQLFromFile;

  private Label wlSQLFromFile;

  private FormData fdlUseSubs, fdUseSubs;

  private FormData fdlSQLFromFile, fdSQLFromFile;

  private Label wlSQL;

  private StyledTextComp wSQL;

  private FormData fdlSQL, fdSQL;

  private Label wlPosition;

  private FormData fdlPosition;

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel;

  private JobEntrySQL jobEntry;

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

  public JobEntrySQLDialog( Shell parent, JobEntryInterface jobEntryInt, JobMeta jobMeta ) {
    super( parent, jobEntryInt, jobMeta );
    jobEntry = (JobEntrySQL) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobSQL.Name.Default" ) );
    }
  }

  public JobEntryInterface open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

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

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

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
    wSQLFromFile = new Button( shell, SWT.CHECK );
    props.setLook( wSQLFromFile );
    wSQLFromFile.setToolTipText( BaseMessages.getString( PKG, "JobSQL.SQLFromFile.Tooltip" ) );
    fdSQLFromFile = new FormData();
    fdSQLFromFile.left = new FormAttachment( middle, 0 );
    fdSQLFromFile.top = new FormAttachment( wConnection, 2 * margin );
    fdSQLFromFile.right = new FormAttachment( 100, 0 );
    wSQLFromFile.setLayoutData( fdSQLFromFile );
    wSQLFromFile.addSelectionListener( new SelectionAdapter() {
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
    fdlFilename.top = new FormAttachment( wSQLFromFile, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wSQLFromFile, margin );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.setToolTipText( BaseMessages.getString( PKG, "JobSQL.Filename.Tooltip" ) );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wSQLFromFile, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wFilename.setToolTipText( jobMeta.environmentSubstitute( wFilename.getText() ) );
      }
    } );

    wbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.sql", "*.txt", "*" } );
        if ( wFilename.getText() != null ) {
          dialog.setFileName( jobMeta.environmentSubstitute( wFilename.getText() ) );
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
    fdlPosition.bottom = new FormAttachment( wOK, -margin );
    wlPosition.setLayoutData( fdlPosition );

    // Script line
    wlSQL = new Label( shell, SWT.NONE );
    wlSQL.setText( BaseMessages.getString( PKG, "JobSQL.Script.Label" ) );
    props.setLook( wlSQL );
    fdlSQL = new FormData();
    fdlSQL.left = new FormAttachment( 0, 0 );
    fdlSQL.top = new FormAttachment( wUseSubs, margin );
    wlSQL.setLayoutData( fdlSQL );

    wSQL =
      new StyledTextComp( jobEntry, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wSQL, Props.WIDGET_STYLE_FIXED );
    wSQL.addModifyListener( lsMod );
    fdSQL = new FormData();
    fdSQL.left = new FormAttachment( 0, 0 );
    fdSQL.top = new FormAttachment( wlSQL, margin );
    fdSQL.right = new FormAttachment( 100, -10 );
    fdSQL.bottom = new FormAttachment( wlPosition, -margin );
    wSQL.setLayoutData( fdSQL );

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

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

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
    wSQL.addModifyListener( lsMod );

    // Text Higlighting
    wSQL.addLineStyleListener( new SQLValuesHighlight() );

    getData();
    activeSQLFromFile();

    BaseStepDialog.setSize( shell );

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

    String scr = wSQL.getText();
    int linenr = wSQL.getLineAtOffset( wSQL.getCaretOffset() ) + 1;
    int posnr = wSQL.getCaretOffset();

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
    wSQL.setText( Const.nullToEmpty( jobEntry.getSQL() ) );
    DatabaseMeta dbinfo = jobEntry.getDatabase();
    if ( dbinfo != null && dbinfo.getName() != null ) {
      wConnection.setText( dbinfo.getName() );
    } else {
      wConnection.setText( "" );
    }

    wUseSubs.setSelection( jobEntry.getUseVariableSubstitution() );
    wSQLFromFile.setSelection( jobEntry.getSQLFromFile() );
    wSendOneStatement.setSelection( jobEntry.isSendOneStatement() );

    wFilename.setText( Const.nullToEmpty( jobEntry.getSQLFilename() ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void activeSQLFromFile() {
    wlFilename.setEnabled( wSQLFromFile.getSelection() );
    wFilename.setEnabled( wSQLFromFile.getSelection() );
    wbFilename.setEnabled( wSQLFromFile.getSelection() );
    wSQL.setEnabled( !wSQLFromFile.getSelection() );
    wlSQL.setEnabled( !wSQLFromFile.getSelection() );
    wlPosition.setEnabled( !wSQLFromFile.getSelection() );

  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setSQL( wSQL.getText() );
    jobEntry.setUseVariableSubstitution( wUseSubs.getSelection() );
    jobEntry.setSQLFromFile( wSQLFromFile.getSelection() );
    jobEntry.setSQLFilename( wFilename.getText() );
    jobEntry.setSendOneStatement( wSendOneStatement.getSelection() );
    jobEntry.setDatabase( jobMeta.findDatabase( wConnection.getText() ) );
    dispose();
  }
}
