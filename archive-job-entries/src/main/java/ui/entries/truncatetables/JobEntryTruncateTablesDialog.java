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

package org.apache.hop.ui.job.entries.truncatetables;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.truncatetables.JobEntryTruncateTables;
import org.apache.hop.job.entry.JobEntryDialogInterface;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.job.dialog.JobDialog;
import org.apache.hop.ui.job.entry.JobEntryDialog;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.Arrays;

/**
 * This dialog allows you to edit the Truncate Tables job entry settings. (select the connection and the table to be
 * truncated)
 *
 * @author Samatar
 * @since 22-07-2008
 */
public class JobEntryTruncateTablesDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntryTruncateTables.class; // for i18n purposes, needed by Translator2!!

  private Button wbTable;

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel;

  private JobEntryTruncateTables jobEntry;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private Button wbdTablename;
  private FormData fdbdTablename;

  private Label wlPrevious;
  private Button wPrevious;
  private FormData fdlPrevious, fdPrevious;

  public JobEntryTruncateTablesDialog( Shell parent, JobEntryInterface jobEntryInt, JobMeta jobMeta ) {
    super( parent, jobEntryInt, jobMeta );
    jobEntry = (JobEntryTruncateTables) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobTruncateTables.Name.Default" ) );
    }
  }

  @Override
  public JobEntryInterface open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobTruncateTables.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobTruncateTables.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
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

    wlPrevious = new Label( shell, SWT.RIGHT );
    wlPrevious.setText( BaseMessages.getString( PKG, "JobTruncateTables.Previous.Label" ) );
    props.setLook( wlPrevious );
    fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment( 0, 0 );
    fdlPrevious.top = new FormAttachment( wConnection, margin );
    fdlPrevious.right = new FormAttachment( middle, -margin );
    wlPrevious.setLayoutData( fdlPrevious );
    wPrevious = new Button( shell, SWT.CHECK );
    props.setLook( wPrevious );
    wPrevious.setToolTipText( BaseMessages.getString( PKG, "JobTruncateTables.Previous.Tooltip" ) );
    fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment( middle, 0 );
    fdPrevious.top = new FormAttachment( wConnection, margin );
    fdPrevious.right = new FormAttachment( 100, 0 );
    wPrevious.setLayoutData( fdPrevious );
    wPrevious.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {

        setPrevious();
        jobEntry.setChanged();
      }
    } );

    wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "JobTruncateTables.GetTablenamesList.Auto" ) );
    FormData fdbTable = new FormData();
    fdbTable.left = new FormAttachment( 0, margin );
    fdbTable.right = new FormAttachment( 100, -margin );
    fdbTable.top = new FormAttachment( wPrevious, 2 * margin );
    wbTable.setLayoutData( fdbTable );
    wbTable.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    // Buttons to the right of the screen...
    wbdTablename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdTablename );
    wbdTablename.setText( BaseMessages.getString( PKG, "JobTruncateTables.TableDelete.Button" ) );
    wbdTablename.setToolTipText( BaseMessages.getString( PKG, "JobTruncateTables.TableDelete.Tooltip" ) );
    fdbdTablename = new FormData();
    fdbdTablename.right = new FormAttachment( 100, 0 );
    fdbdTablename.top = new FormAttachment( wbTable, 2 * middle );
    wbdTablename.setLayoutData( fdbdTablename );

    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobTruncateTables.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wbTable, 2 * margin );
    wlFields.setLayoutData( fdlFields );

    int rows = jobEntry.arguments == null ? 1 : ( jobEntry.arguments.length == 0 ? 0 : jobEntry.arguments.length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobTruncateTables.Fields.Table.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobTruncateTables.Fields.Schema.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "JobTruncateTables.Fields.Table.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "JobTruncateTables.Fields.Schema.Tooltip" ) );

    wFields =
      new TableView(
        jobMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wbdTablename, -margin );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    // Delete files from the list of files...
    wbdTablename.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 50, -10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wOK.setLayoutData( fd );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 50, 10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wCancel.setLayoutData( fd );

    // Add listeners
    lsCancel = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );
    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wFields );
    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setPrevious();
    BaseStepDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobTruncateTablesDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  private void setPrevious() {
    wlFields.setEnabled( !wPrevious.getSelection() );
    wFields.setEnabled( !wPrevious.getSelection() );
    wbdTablename.setEnabled( !wPrevious.getSelection() );
    wbTable.setEnabled( !wPrevious.getSelection() );
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
    if ( jobEntry.getDatabase() != null ) {
      wConnection.setText( jobEntry.getDatabase().getName() );
    }
    if ( jobEntry.arguments != null ) {
      for ( int i = 0; i < jobEntry.arguments.length; i++ ) {
        // TableItem ti = new TableItem(wFields.table, SWT.NONE);
        TableItem ti = wFields.table.getItem( i );
        if ( jobEntry.arguments[ i ] != null ) {
          ti.setText( 1, jobEntry.arguments[ i ] );
        }
        if ( jobEntry.schemaname[ i ] != null ) {
          ti.setText( 2, jobEntry.schemaname[ i ] );
        }
      }

      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth( true );
    }
    wPrevious.setSelection( jobEntry.argFromPrevious );

    wName.selectAll();
    wName.setFocus();
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
    jobEntry.setDatabase( jobMeta.findDatabase( wConnection.getText() ) );
    jobEntry.argFromPrevious = wPrevious.getSelection();

    int nritems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    jobEntry.arguments = new String[ nr ];
    jobEntry.schemaname = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      String wild = wFields.getNonEmpty( i ).getText( 2 );
      if ( arg != null && arg.length() != 0 ) {
        jobEntry.arguments[ nr ] = arg;
        jobEntry.schemaname[ nr ] = wild;
        nr++;
      }
    }

    dispose();
  }

  private void getTableName() {
    DatabaseMeta databaseMeta = jobMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      Database database = new Database( loggingObject, databaseMeta );
      try {
        database.connect();
        String[] Tablenames = database.getTablenames();
        Arrays.sort( Tablenames );
        EnterSelectionDialog dialog = new EnterSelectionDialog( shell, Tablenames,
          BaseMessages.getString( PKG, "JobTruncateTables.SelectTables.Title" ),
          BaseMessages.getString( PKG, "JobTruncateTables.SelectTables.Message" ) );
        dialog.setMulti( true );
        dialog.setAvoidQuickSearch();
        if ( dialog.open() != null ) {
          int[] idx = dialog.getSelectionIndeces();
          for ( int i = 0; i < idx.length; i++ ) {
            TableItem tableItem = new TableItem( wFields.table, SWT.NONE );
            tableItem.setText( 1, Tablenames[ idx[ i ] ] );
          }
        }
      } catch ( HopDatabaseException e ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "System.Dialog.Error.Title" ),
          BaseMessages.getString( PKG, "JobEntryTruncateTables.ConnectionError.DialogMessage" ), e );
      } finally {
        if ( database != null ) {
          database.disconnect();
        }
      }
      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth( true );

    }

  }
}
