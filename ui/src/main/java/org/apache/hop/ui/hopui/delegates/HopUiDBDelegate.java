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

package org.apache.hop.ui.hopui.delegates;

import java.util.List;

import org.apache.hop.ui.hopui.HopUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.DBCache;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.UndoInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.HasDatabasesInterface;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.selectvalues.SelectMetadataChange;
import org.apache.hop.trans.steps.selectvalues.SelectValuesMeta;
import org.apache.hop.trans.steps.tableinput.TableInputMeta;
import org.apache.hop.trans.steps.tableoutput.TableOutputMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseDialog;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SQLEditor;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SQLStatementsDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopui.SharedObjectSyncUtil;
import org.apache.hop.ui.hopui.dialog.GetJobSQLProgressDialog;
import org.apache.hop.ui.hopui.dialog.GetSQLProgressDialog;
import org.apache.hop.ui.hopui.tree.provider.DBConnectionFolderProvider;

public class HopUiDBDelegate extends HopUiDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!
  private DatabaseDialog databaseDialog;
  private SharedObjectSyncUtil sharedObjectSyncUtil;

  public HopUiDBDelegate( HopUi hopUi ) {
    super( hopUi );
  }

  public void setSharedObjectSyncUtil( SharedObjectSyncUtil sharedObjectSyncUtil ) {
    this.sharedObjectSyncUtil = sharedObjectSyncUtil;
  }

  public void sqlConnection( DatabaseMeta databaseMeta ) {
    SQLEditor sql =
      new SQLEditor( databaseMeta, hopUi.getShell(), SWT.NONE, databaseMeta, DBCache.getInstance(), "" );
    sql.open();
  }

  public void editConnection( DatabaseMeta databaseMeta ) {
    HasDatabasesInterface hasDatabasesInterface = hopUi.getActiveHasDatabasesInterface();
    if ( hasDatabasesInterface == null ) {
      return; // program error, exit just to make sure.
    }

    String originalName = databaseMeta.getName();
    getDatabaseDialog().setDatabaseMeta( databaseMeta );
    getDatabaseDialog().setDatabases( hasDatabasesInterface.getDatabases() );
    String newname = getDatabaseDialog().open();
    if ( !Utils.isEmpty( newname ) ) { // null: CANCEL
      databaseMeta.setName( originalName );

      databaseMeta = getDatabaseDialog().getDatabaseMeta();
      if ( !newname.equals( originalName )
          && databaseMeta.findDatabase( hasDatabasesInterface.getDatabases(), newname ) != null ) {
        databaseMeta.setName( newname.trim() );
        DatabaseDialog.showDatabaseExistsDialog( hopUi.getShell(), databaseMeta );
        databaseMeta.setName( originalName );
        databaseMeta.setDisplayName( originalName );
        return;
      }
      databaseMeta.setName( newname.trim() );
      databaseMeta.setDisplayName( newname.trim() );
      saveConnection( databaseMeta, Const.VERSION_COMMENT_EDIT_VERSION );
      if ( databaseMeta.isShared() ) {
        sharedObjectSyncUtil.synchronizeConnections( databaseMeta, originalName );
      }

      saveConnection( databaseMeta, Const.VERSION_COMMENT_EDIT_VERSION );
      if ( databaseMeta.isShared() ) {
        sharedObjectSyncUtil.synchronizeConnections( databaseMeta, originalName );
      }

      refreshTree();
    }
    hopUi.setShellText();
  }

  private DatabaseDialog getDatabaseDialog() {
    if ( databaseDialog != null ) {
      return databaseDialog;
    }
    databaseDialog = new DatabaseDialog( hopUi.getShell() );
    return databaseDialog;
  }

  public void dupeConnection( HasDatabasesInterface hasDatabasesInterface, DatabaseMeta databaseMeta ) {
    String name = databaseMeta.getName();
    int pos = hasDatabasesInterface.indexOfDatabase( databaseMeta );
    if ( databaseMeta != null ) {
      DatabaseMeta databaseMetaCopy = (DatabaseMeta) databaseMeta.clone();
      String dupename = BaseMessages.getString( PKG, "Spoon.Various.DupeName" ) + name;
      databaseMetaCopy.setName( dupename );

      getDatabaseDialog().setDatabaseMeta( databaseMetaCopy );

      String newname = getDatabaseDialog().open();
      if ( newname != null ) { // null: CANCEL

        databaseMetaCopy.verifyAndModifyDatabaseName( hasDatabasesInterface.getDatabases(), name );
        hasDatabasesInterface.addDatabase( pos + 1, databaseMetaCopy );
        hopUi.addUndoNew(
          (UndoInterface) hasDatabasesInterface, new DatabaseMeta[] { (DatabaseMeta) databaseMetaCopy.clone() },
          new int[] { pos + 1 } );
        saveConnection( databaseMetaCopy, Const.VERSION_COMMENT_EDIT_VERSION );
        refreshTree();
      }
    }
  }

  public void clipConnection( DatabaseMeta databaseMeta ) {
    String xml = XMLHandler.getXMLHeader() + databaseMeta.getXML();
    GUIResource.getInstance().toClipboard( xml );
  }

  /**
   * Delete a database connection
   *
   * @param name
   *          The name of the database connection.
   */
  public void delConnection( HasDatabasesInterface hasDatabasesInterface, DatabaseMeta db ) {
    int pos = hasDatabasesInterface.indexOfDatabase( db );
    boolean worked = false;

    // delete from repository?
    Repository rep = hopUi.getRepository();
    if ( rep != null ) {
      if ( !rep.getSecurityProvider().isReadOnly() ) {
        try {
          rep.deleteDatabaseMeta( db.getName() );
          worked = true;
        } catch ( HopException dbe ) {
          new ErrorDialog( hopUi.getShell(),
            BaseMessages.getString( PKG, "Spoon.Dialog.ErrorDeletingConnection.Title" ),
            BaseMessages.getString( PKG, "Spoon.Dialog.ErrorDeletingConnection.Message", db.getName() ), dbe );
        }
      } else {
        new ErrorDialog( hopUi.getShell(),
          BaseMessages.getString( PKG, "Spoon.Dialog.ErrorDeletingConnection.Title" ),
          BaseMessages.getString( PKG, "Spoon.Dialog.ErrorDeletingConnection.Message", db.getName() ),
          new HopException( BaseMessages.getString( PKG, "Spoon.Dialog.Exception.ReadOnlyUser" ) ) );
      }
    }

    if ( hopUi.getRepository() == null || worked ) {
      hopUi.addUndoDelete(
        (UndoInterface) hasDatabasesInterface, new DatabaseMeta[] { (DatabaseMeta) db.clone() },
        new int[] { pos } );
      hasDatabasesInterface.removeDatabase( pos );
      DBCache.getInstance().clear( db.getName() ); // remove this from the cache as well.
    }

    refreshTree();
    hopUi.setShellText();
  }

  /**
   * return a schema, table combination from the explorer
   *
   * @param databaseMeta
   * @param aLook
   * @return schema [0] and table [1]
   */
  public String[] exploreDB( DatabaseMeta databaseMeta, boolean aLook ) {
    List<DatabaseMeta> databases = null;
    HasDatabasesInterface activeHasDatabasesInterface = hopUi.getActiveHasDatabasesInterface();
    if ( activeHasDatabasesInterface != null ) {
      databases = activeHasDatabasesInterface.getDatabases();
    }

    DatabaseExplorerDialog std =
      new DatabaseExplorerDialog( hopUi.getShell(), SWT.NONE, databaseMeta, databases, aLook );
    std.open();
    return new String[] { std.getSchemaName(), std.getTableName() };
  }

  public void clearDBCache( DatabaseMeta databaseMeta ) {
    if ( databaseMeta != null ) {
      DBCache.getInstance().clear( databaseMeta.getName() );
    } else {
      DBCache.getInstance().clear( null );
    }
  }

  public void getSQL() {
    TransMeta transMeta = hopUi.getActiveTransformation();
    if ( transMeta != null ) {
      getTransSQL( transMeta );
    }
    JobMeta jobMeta = hopUi.getActiveJob();
    if ( jobMeta != null ) {
      getJobSQL( jobMeta );
    }
  }

  /**
   * Get & show the SQL required to run the loaded transformation...
   *
   */
  public void getTransSQL( TransMeta transMeta ) {
    GetSQLProgressDialog pspd = new GetSQLProgressDialog( hopUi.getShell(), transMeta );
    List<SQLStatement> stats = pspd.open();
    if ( stats != null ) {
      // null means error, but we already displayed the error

      if ( stats.size() > 0 ) {
        SQLStatementsDialog ssd =
          new SQLStatementsDialog( hopUi.getShell(), Variables.getADefaultVariableSpace(), SWT.NONE, stats );
        String sn = ssd.open();

        if ( sn != null ) {
          StepMeta esi = transMeta.findStep( sn );
          if ( esi != null ) {
            hopUi.delegates.steps.editStep( transMeta, esi );
          }
        }
      } else {
        MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.NoSQLNeedEexecuted.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.NoSQLNeedEexecuted.Title" ) ); // "SQL"
        mb.open();
      }
    }
  }

  /**
   * Get & show the SQL required to run the loaded job entry...
   *
   */
  public void getJobSQL( JobMeta jobMeta ) {
    GetJobSQLProgressDialog pspd = new GetJobSQLProgressDialog( hopUi.getShell(), jobMeta, hopUi.getRepository() );
    List<SQLStatement> stats = pspd.open();
    if ( stats != null ) {
      // null means error, but we already displayed the error

      if ( stats.size() > 0 ) {
        SQLStatementsDialog ssd = new SQLStatementsDialog( hopUi.getShell(), jobMeta, SWT.NONE, stats );
        ssd.open();
      } else {
        MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.JobNoSQLNeedEexecuted.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.JobNoSQLNeedEexecuted.Title" ) );
        mb.open();
      }
    }
  }

  public boolean copyTable( DatabaseMeta sourceDBInfo, DatabaseMeta targetDBInfo, String tablename ) {
    try {
      //
      // Create a new transformation...
      //
      TransMeta meta = new TransMeta();
      meta.addDatabase( sourceDBInfo );
      meta.addDatabase( targetDBInfo );

      //
      // Add a note
      //
      String note =
        BaseMessages.getString( PKG, "Spoon.Message.Note.ReadInformationFromTableOnDB", tablename, sourceDBInfo
          .getDatabaseName() )
          + Const.CR; // "Reads information from table ["+tablename+"]
      // on database ["+sourceDBInfo+"]"
      note +=
        BaseMessages.getString( PKG, "Spoon.Message.Note.WriteInformationToTableOnDB", tablename, targetDBInfo
          .getDatabaseName() ); // "After that, it writes
      // the information to table
      // ["+tablename+"] on
      // database
      // ["+targetDBInfo+"]"
      NotePadMeta ni = new NotePadMeta( note, 150, 10, -1, -1 );
      meta.addNote( ni );

      //
      // create the source step...
      //
      String fromstepname = BaseMessages.getString( PKG, "Spoon.Message.Note.ReadFromTable", tablename ); // "read
      // from
      // ["+tablename+"]";
      TableInputMeta tii = new TableInputMeta();
      tii.setDatabaseMeta( sourceDBInfo );
      tii.setSQL( "SELECT * FROM " + tablename );

      PluginRegistry registry = PluginRegistry.getInstance();

      String fromstepid = registry.getPluginId( StepPluginType.class, tii );
      StepMeta fromstep = new StepMeta( fromstepid, fromstepname, tii );
      fromstep.setLocation( 150, 100 );
      fromstep.setDraw( true );
      fromstep.setDescription( BaseMessages.getString(
        PKG, "Spoon.Message.Note.ReadInformationFromTableOnDB", tablename, sourceDBInfo.getDatabaseName() ) );
      meta.addStep( fromstep );

      //
      // add logic to rename fields in case any of the field names contain
      // reserved words...
      // Use metadata logic in SelectValues, use SelectValueInfo...
      //
      Database sourceDB = new Database( loggingObject, sourceDBInfo );
      sourceDB.shareVariablesWith( meta );
      sourceDB.connect();
      try {
        // Get the fields for the input table...
        RowMetaInterface fields = sourceDB.getTableFields( tablename );

        // See if we need to deal with reserved words...
        int nrReserved = targetDBInfo.getNrReservedWords( fields );
        if ( nrReserved > 0 ) {
          SelectValuesMeta svi = new SelectValuesMeta();
          svi.allocate( 0, 0, nrReserved );
          int nr = 0;
          //CHECKSTYLE:Indentation:OFF
          for ( int i = 0; i < fields.size(); i++ ) {
            ValueMetaInterface v = fields.getValueMeta( i );
            if ( targetDBInfo.isReservedWord( v.getName() ) ) {
              if ( svi.getMeta()[nr] == null ) {
                svi.getMeta()[nr] = new SelectMetadataChange( svi );
              }
              svi.getMeta()[nr].setName( v.getName() );
              svi.getMeta()[nr].setRename( targetDBInfo.quoteField( v.getName() ) );
              nr++;
            }
          }

          String selstepname = BaseMessages.getString( PKG, "Spoon.Message.Note.HandleReservedWords" );
          String selstepid = registry.getPluginId( StepPluginType.class, svi );
          StepMeta selstep = new StepMeta( selstepid, selstepname, svi );
          selstep.setLocation( 350, 100 );
          selstep.setDraw( true );
          selstep.setDescription( BaseMessages.getString(
            PKG, "Spoon.Message.Note.RenamesReservedWords", targetDBInfo.getPluginId() ) ); //
          meta.addStep( selstep );

          TransHopMeta shi = new TransHopMeta( fromstep, selstep );
          meta.addTransHop( shi );
          fromstep = selstep;
        }

        //
        // Create the target step...
        //
        //
        // Add the TableOutputMeta step...
        //
        String tostepname = BaseMessages.getString( PKG, "Spoon.Message.Note.WriteToTable", tablename );
        TableOutputMeta toi = new TableOutputMeta();
        toi.setDatabaseMeta( targetDBInfo );
        toi.setTableName( tablename );
        toi.setCommitSize( 200 );
        toi.setTruncateTable( true );

        String tostepid = registry.getPluginId( StepPluginType.class, toi );
        StepMeta tostep = new StepMeta( tostepid, tostepname, toi );
        tostep.setLocation( 550, 100 );
        tostep.setDraw( true );
        tostep.setDescription( BaseMessages.getString(
          PKG, "Spoon.Message.Note.WriteInformationToTableOnDB2", tablename, targetDBInfo.getDatabaseName() ) );
        meta.addStep( tostep );

        //
        // Add a hop between the two steps...
        //
        TransHopMeta hi = new TransHopMeta( fromstep, tostep );
        meta.addTransHop( hi );

        // OK, if we're still here: overwrite the current transformation...
        // Set a name on this generated transformation
        //
        String name = "Copy table from [" + sourceDBInfo.getName() + "] to [" + targetDBInfo.getName() + "]";
        String transName = name;
        int nr = 1;
        if ( hopUi.delegates.trans.getTransformation( transName ) != null ) {
          nr++;
          transName = name + " " + nr;
        }
        meta.setName( transName );
        hopUi.delegates.trans.addTransGraph( meta );

        hopUi.refreshGraph();
        refreshTree();
      } finally {
        sourceDB.disconnect();
      }
    } catch ( Exception e ) {
      new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.UnexpectedError.Title" ), BaseMessages
          .getString( PKG, "Spoon.Dialog.UnexpectedError.Message" ), new HopException( e.getMessage(), e ) );
      return false;
    }
    return true;
  }

  public void saveConnection( DatabaseMeta db, String versionComment ) {
    // Also add to repository?
    Repository rep = hopUi.getRepository();

    if ( rep != null ) {
      if ( !rep.getSecurityProvider().isReadOnly() ) {
        try {

          if ( Utils.isEmpty( versionComment ) ) {
            rep.insertLogEntry( "Saving database '" + db.getName() + "'" );
          } else {
            rep.insertLogEntry( "Save database : " + versionComment );
          }
          rep.save( db, versionComment, null );
          hopUi.getLog().logDetailed(
            BaseMessages.getString( PKG, "Spoon.Log.SavedDatabaseConnection", db.getDatabaseName() ) );

          db.setChanged( false );
        } catch ( HopException ke ) {
          new ErrorDialog( hopUi.getShell(),
            BaseMessages.getString( PKG, "Spoon.Dialog.ErrorSavingConnection.Title" ),
            BaseMessages.getString( PKG, "Spoon.Dialog.ErrorSavingConnection.Message", db.getDatabaseName() ), ke );
        }
      } else {
        // This repository user is read-only!
        //
        new ErrorDialog(
          hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.UnableSave.Title" ), BaseMessages
            .getString( PKG, "Spoon.Dialog.ErrorSavingConnection.Message", db.getDatabaseName() ),
          new HopException( BaseMessages.getString( PKG, "Spoon.Dialog.Exception.ReadOnlyRepositoryUser" ) ) );
      }
    }

  }

  public void newConnection() {
    HasDatabasesInterface hasDatabasesInterface = hopUi.getActiveHasDatabasesInterface();
    if ( hasDatabasesInterface == null && hopUi.rep == null ) {
      return;
    }
    newConnection( hasDatabasesInterface );
  }

  public void newConnection( HasDatabasesInterface hasDatabasesInterface ) {

    DatabaseMeta databaseMeta = new DatabaseMeta();
    if ( hasDatabasesInterface instanceof VariableSpace ) {
      databaseMeta.shareVariablesWith( (VariableSpace) hasDatabasesInterface );
    } else {
      databaseMeta.initializeVariablesFrom( null );
    }

    getDatabaseDialog().setDatabaseMeta( databaseMeta );
    String con_name = getDatabaseDialog().open();
    if ( !Utils.isEmpty( con_name ) ) {
      con_name = con_name.trim();
      databaseMeta.setName( con_name );
      databaseMeta.setDisplayName( con_name );
      databaseMeta = getDatabaseDialog().getDatabaseMeta();

      if ( databaseMeta.findDatabase( hasDatabasesInterface.getDatabases(), con_name ) == null ) {
        hasDatabasesInterface.addDatabase( databaseMeta );
        hopUi.addUndoNew( (UndoInterface) hasDatabasesInterface, new DatabaseMeta[]{(DatabaseMeta) databaseMeta
                .clone()}, new int[]{hasDatabasesInterface.indexOfDatabase( databaseMeta )} );
        if ( hopUi.rep != null ) {
          try {
            if ( !hopUi.rep.getSecurityProvider().isReadOnly() ) {
              // spoon.rep.getDatabaseID(  )
              hopUi.rep.save( databaseMeta, Const.VERSION_COMMENT_INITIAL_VERSION, null );
            } else {
              throw new HopException( BaseMessages.getString(
                      PKG, "Spoon.Dialog.Exception.ReadOnlyRepositoryUser" ) );
            }
          } catch ( HopException e ) {
            new ErrorDialog( hopUi.getShell(),
                    BaseMessages.getString( PKG, "Spoon.Dialog.ErrorSavingConnection.Title" ),
                    BaseMessages.getString( PKG, "Spoon.Dialog.ErrorSavingConnection.Message", databaseMeta.getName() ), e );
          }
        }
        refreshTree();
      } else {
        DatabaseDialog.showDatabaseExistsDialog( hopUi.getShell(), databaseMeta );
      }
    }
  }

  private void refreshTree() {
    hopUi.refreshTree( DBConnectionFolderProvider.STRING_CONNECTIONS );
  }
}
