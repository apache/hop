/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.columnsexist;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.columnsexist.JobEntryColumnsExist;
import org.apache.hop.job.entry.JobEntryDialogInterface;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
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

/**
 * This dialog allows you to edit the Column Exists job entry settings. (select the connection and the table to be
 * checked) This entry type evaluates!
 *
 * @author Samatar
 * @since 15-06-2008
 */
@PluginDialog( 
		  id = "COLUMNS_EXIST", 
		  image = "ColumnsExist.svg", 
		  pluginType = PluginDialog.PluginType.JOBENTRY,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class JobEntryColumnsExistDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntryColumnsExist.class; // for i18n purposes, needed by Translator2!!

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlTablename;

  private TextVar wTablename;

  private FormData fdlTablename, fdTablename;

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel;

  private JobEntryColumnsExist jobEntry;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Button wbTable, wbGetColumns;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields, fdbGetColumns;

  // Schema name
  private Label wlSchemaname;
  private TextVar wSchemaname;
  private FormData fdlSchemaname, fdSchemaname;

  private Button wbdFilename; // Delete
  private FormData fdbdFilename;

  private FormData fdbSchema;
  private Button wbSchema;

  public JobEntryColumnsExistDialog( Shell parent, JobEntryInterface jobEntryInt, JobMeta jobMeta ) {
    super( parent, jobEntryInt, jobMeta );
    jobEntry = (JobEntryColumnsExist) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobEntryColumnsExist.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobEntryColumnsExist.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobEntryColumnsExist.Name.Label" ) );
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

    // Schema name line
    wlSchemaname = new Label( shell, SWT.RIGHT );
    wlSchemaname.setText( BaseMessages.getString( PKG, "JobEntryColumnsExist.Schemaname.Label" ) );
    props.setLook( wlSchemaname );
    fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment( 0, 0 );
    fdlSchemaname.right = new FormAttachment( middle, -margin );
    fdlSchemaname.top = new FormAttachment( wConnection, 2 * margin );
    wlSchemaname.setLayoutData( fdlSchemaname );

    wbSchema = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSchema );
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData( fdbSchema );
    wbSchema.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getSchemaNames();
      }
    } );

    wSchemaname = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaname );
    wSchemaname.setToolTipText( BaseMessages.getString( PKG, "JobEntryColumnsExist.Schemaname.Tooltip" ) );
    wSchemaname.addModifyListener( lsMod );
    fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment( middle, 0 );
    fdSchemaname.top = new FormAttachment( wConnection, 2 * margin );
    fdSchemaname.right = new FormAttachment( wbSchema, -margin );
    wSchemaname.setLayoutData( fdSchemaname );

    // Table name line
    wlTablename = new Label( shell, SWT.RIGHT );
    wlTablename.setText( BaseMessages.getString( PKG, "JobEntryColumnsExist.Tablename.Label" ) );
    props.setLook( wlTablename );
    fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment( 0, 0 );
    fdlTablename.right = new FormAttachment( middle, -margin );
    fdlTablename.top = new FormAttachment( wbSchema, margin );
    wlTablename.setLayoutData( fdlTablename );

    wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wbSchema, margin );
    wbTable.setLayoutData( fdbTable );
    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    // Table name
    wTablename = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTablename );
    wTablename.addModifyListener( lsMod );
    fdTablename = new FormData();
    fdTablename.left = new FormAttachment( middle, 0 );
    fdTablename.top = new FormAttachment( wbSchema, margin );
    fdTablename.right = new FormAttachment( wbTable, -margin );
    wTablename.setLayoutData( fdTablename );

    // Get columns button
    wbGetColumns = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGetColumns );
    wbGetColumns.setText( BaseMessages.getString( PKG, "JobEntryColumnsExist.GetColums.Button" ) );
    wbGetColumns.setToolTipText( BaseMessages.getString( PKG, "JobEntryColumnsExist.GetColums.Tooltip" ) );
    fdbGetColumns = new FormData();
    fdbGetColumns.right = new FormAttachment( 100, 0 );
    fdbGetColumns.top = new FormAttachment( wTablename, 38 );
    wbGetColumns.setLayoutData( fdbGetColumns );

    // Buttons to the right of the screen...
    wbdFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdFilename );
    wbdFilename.setText( BaseMessages.getString( PKG, "JobEntryColumnsExist.FilenameDelete.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "JobEntryColumnsExist.FilenameDelete.Tooltip" ) );
    fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wbGetColumns, margin );
    wbdFilename.setLayoutData( fdbdFilename );

    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobEntryColumnsExist.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wTablename, 3 * margin );
    wlFields.setLayoutData( fdlFields );

    int rows = jobEntry.getArguments() == null ? 1
      : ( jobEntry.getArguments().length == 0 ? 0 : jobEntry.getArguments().length );

    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] { new ColumnInfo( BaseMessages.getString(
        PKG, "JobEntryColumnsExist.Fields.Argument.Label" ), ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "JobEntryColumnsExist.Fields.Column" ) );

    wFields =
      new TableView(
        jobMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wbGetColumns, -margin );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    // Delete files from the list of files...
    wbdFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // Delete files from the list of files...
    wbGetColumns.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        getListColumns();
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
    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wFields );
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );
    wTablename.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobEntryColumnsExistDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  private void getTableName() {
    String databaseName = wConnection.getText();
    if ( StringUtils.isNotEmpty( databaseName ) ) {
      DatabaseMeta databaseMeta = jobMeta.findDatabase( databaseName );
      if ( databaseMeta != null ) {
        DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, databaseMeta, jobMeta.getDatabases() );
        std.setSelectedSchemaAndTable( wSchemaname.getText(), wTablename.getText() );
        if ( std.open() ) {
          wSchemaname.setText( Const.NVL( std.getSchemaName(), "" ) );
          wTablename.setText( Const.NVL( std.getTableName(), "" ) );
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( BaseMessages.getString( PKG, "JobEntryColumnsExist.ConnectionError.DialogMessage" ) );
        mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
        mb.open();
      }
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( jobEntry.getName() != null ) {
      wName.setText( jobEntry.getName() );
    }
    if ( jobEntry.getTablename() != null ) {
      wTablename.setText( jobEntry.getTablename() );
    }

    if ( jobEntry.getSchemaname() != null ) {
      wSchemaname.setText( jobEntry.getSchemaname() );
    }

    if ( jobEntry.getDatabase() != null ) {
      wConnection.setText( jobEntry.getDatabase().getName() );
    }

    if ( jobEntry.getArguments() != null ) {
      for ( int i = 0; i < jobEntry.getArguments().length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( jobEntry.getArguments()[ i ] != null ) {
          ti.setText( 1, jobEntry.getArguments()[ i ] );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }

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
    jobEntry.setTablename( wTablename.getText() );
    jobEntry.setSchemaname( wSchemaname.getText() );

    int nritems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    String[] args = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        args[ nr ] = arg;
        nr++;
      }
    }
    jobEntry.setArguments( args );

    dispose();
  }

  /**
   * Get a list of columns
   */
  private void getListColumns() {
    if ( !Utils.isEmpty( wTablename.getText() ) ) {
      DatabaseMeta databaseMeta = jobMeta.findDatabase( wConnection.getText() );
      if ( databaseMeta != null ) {
        Database database = new Database( loggingObject, databaseMeta );
        database.shareVariablesWith( jobMeta );
        try {
          database.connect();
          RowMetaInterface row =
            database.getTableFieldsMeta(
              jobMeta.environmentSubstitute( wSchemaname.getText() ),
              jobMeta.environmentSubstitute( wTablename.getText() ) );
          if ( row != null ) {
            String[] available = row.getFieldNames();

            wFields.removeAll();
            for ( int i = 0; i < available.length; i++ ) {
              wFields.add( available[ i ] );
            }
            wFields.removeEmptyRows();
            wFields.setRowNums();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( PKG, "JobEntryColumnsExist.GetListColumsNoRow.DialogMessage" ) );
            mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
            mb.open();
          }
        } catch ( Exception e ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
            .getString( PKG, "JobEntryColumnsExist.ConnectionError2.DialogMessage", wTablename.getText() ), e );
        } finally {
          database.disconnect();
        }
      }
    }
  }

  private void getSchemaNames() {
    if ( wSchemaname.isDisposed() ) {
      return;
    }
    DatabaseMeta databaseMeta = jobMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      Database database = new Database( loggingObject, databaseMeta );
      database.shareVariablesWith( jobMeta );
      try {
        database.connect();
        String[] schemas = database.getSchemas();

        if ( null != schemas && schemas.length > 0 ) {
          schemas = Const.sortStrings( schemas );
          EnterSelectionDialog dialog = new EnterSelectionDialog( shell, schemas,
            BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Title", wConnection.getText() ),
            BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Message" ) );
          String d = dialog.open();
          if ( d != null ) {
            wSchemaname.setText( Const.NVL( d.toString(), "" ) );
          }

        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Empty.Message" ) );
          mb.setText( BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Empty.Title" ) );
          mb.open();
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.AvailableSchemas.ConnectionError" ), e );
      } finally {
        if ( database != null ) {
          database.disconnect();
          database = null;
        }
      }
    }
  }
}
