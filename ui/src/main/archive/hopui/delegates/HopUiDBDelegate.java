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

import org.apache.hop.core.DBCache;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.gui.UndoInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.trans.HasDatabasesInterface;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseDialog;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SQLEditor;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SQLStatementsDialog;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.dialog.GetJobSQLProgressDialog;
import org.apache.hop.ui.hopui.dialog.GetSQLProgressDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

import java.util.List;

public class HopUiDBDelegate extends HopUiDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!
  private DatabaseDialog databaseDialog;

  public HopUiDBDelegate( HopUi hopUi ) {
    super( hopUi );
  }

  public void sqlConnection( DatabaseMeta databaseMeta ) {
    SQLEditor sql =
      new SQLEditor( databaseMeta, hopUi.getShell(), SWT.NONE, databaseMeta, DBCache.getInstance(), "" );
    sql.open();
  }

  public void editConnection( DatabaseMeta databaseMeta ) {
    try {
      String originalName = databaseMeta.getName();
      getDatabaseDialog().setDatabaseMeta( databaseMeta );
      String newname = getDatabaseDialog().open();
      if ( !Utils.isEmpty( newname ) ) { // null: CANCEL
        databaseMeta.setName( originalName );

        databaseMeta = getDatabaseDialog().getDatabaseMeta();
        if ( !newname.equals( originalName )
          && databaseMeta.loadDatabase( hopUi.getMetaStore(), newname ) != null ) {
          databaseMeta.setName( newname.trim() );
          DatabaseDialog.showDatabaseExistsDialog( hopUi.getShell(), databaseMeta );
          databaseMeta.setName( originalName );
          return;
        }
        databaseMeta.setName( newname.trim() );
        saveConnection( databaseMeta );

        refreshTree();
      }
      hopUi.setShellText();
    } catch ( Exception e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error editing connection", e );
    }
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
      String dupename = BaseMessages.getString( PKG, "HopGui.Various.DupeName" ) + name;
      databaseMetaCopy.setName( dupename );

      getDatabaseDialog().setDatabaseMeta( databaseMetaCopy );

      String newname = getDatabaseDialog().open();
      if ( newname != null ) { // null: CANCEL

        databaseMetaCopy.verifyAndModifyDatabaseName( hasDatabasesInterface.getDatabases(), name );
        hasDatabasesInterface.addDatabase( pos + 1, databaseMetaCopy );
        hopUi.addUndoNew(
          (UndoInterface) hasDatabasesInterface, new DatabaseMeta[] { (DatabaseMeta) databaseMetaCopy.clone() },
          new int[] { pos + 1 } );
        saveConnection( databaseMetaCopy );
        refreshTree();
      }
    }
  }

  /**
   * Delete a database connection
   *
   * @param hasDatabasesInterface The container of the databases
   * @param db                    The database to delete
   *                              The name of the database connection.
   */
  public void delConnection( HasDatabasesInterface hasDatabasesInterface, DatabaseMeta db ) {
    int pos = hasDatabasesInterface.indexOfDatabase( db );
    boolean worked = false;

    hopUi.addUndoDelete( (UndoInterface) hasDatabasesInterface, new DatabaseMeta[] { (DatabaseMeta) db.clone() }, new int[] { pos } );
    hasDatabasesInterface.removeDatabase( pos );
    DBCache.getInstance().clear( db.getName() ); // remove this from the cache as well.

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
    try {
      List<DatabaseMeta> databases = DatabaseMeta.createFactory( hopUi.getMetaStore() ).getElements();
      DatabaseExplorerDialog std = new DatabaseExplorerDialog( hopUi.getShell(), SWT.NONE, databaseMeta, databases, aLook );
      std.open();
      return new String[] { std.getSchemaName(), std.getTableName() };
    } catch ( Exception e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error exploring database", e );
      return new String[] {};
    }
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
        mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.NoSQLNeedEexecuted.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.NoSQLNeedEexecuted.Title" ) ); // "SQL"
        mb.open();
      }
    }
  }

  /**
   * Get & show the SQL required to run the loaded job entry...
   */
  public void getJobSQL( JobMeta jobMeta ) {
    GetJobSQLProgressDialog pspd = new GetJobSQLProgressDialog( hopUi.getShell(), jobMeta );
    List<SQLStatement> stats = pspd.open();
    if ( stats != null ) {
      // null means error, but we already displayed the error

      if ( stats.size() > 0 ) {
        SQLStatementsDialog ssd = new SQLStatementsDialog( hopUi.getShell(), jobMeta, SWT.NONE, stats );
        ssd.open();
      } else {
        MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.JobNoSQLNeedEexecuted.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.JobNoSQLNeedEexecuted.Title" ) );
        mb.open();
      }
    }
  }

  public void saveConnection( DatabaseMeta db ) {

    // Save in the metastore
    //
    IMetaStore metaStore = hopUi.getMetaStore();

    MetaStoreFactory<DatabaseMeta> factory = DatabaseMeta.createFactory( metaStore );

    try {

      factory.saveElement( db );
      hopUi.getLog().logDetailed(
        BaseMessages.getString( PKG, "HopGui.Log.SavedDatabaseConnection", db.getDatabaseName() ) );

      db.setChanged( false );
    } catch ( MetaStoreException me ) {
      new ErrorDialog( hopUi.getShell(),
        BaseMessages.getString( PKG, "HopGui.Dialog.ErrorSavingConnection.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.ErrorSavingConnection.Message", db.getDatabaseName() ), me );
    }


  }

  public void newConnection() throws HopXMLException {
    VariableSpace space = hopUi.getActiveVariableSpace();
    newConnection( space );
  }

  public void newConnection( VariableSpace space ) throws HopXMLException {

    DatabaseMeta databaseMeta = new DatabaseMeta();
    if ( space != null ) {
      databaseMeta.shareVariablesWith( space );
    } else {
      databaseMeta.initializeVariablesFrom( null );
    }

    getDatabaseDialog().setDatabaseMeta( databaseMeta );
    String con_name = getDatabaseDialog().open();
    if ( !Utils.isEmpty( con_name ) ) {
      con_name = con_name.trim();
      databaseMeta.setName( con_name );
      databaseMeta = getDatabaseDialog().getDatabaseMeta();

      DatabaseMeta check = databaseMeta.loadDatabase( hopUi.getMetaStore(), con_name );
      if ( check == null ) {
        saveConnection( databaseMeta );
        refreshTree();
      } else {
        DatabaseDialog.showDatabaseExistsDialog( hopUi.getShell(), databaseMeta );
      }
    }
  }

  private void refreshTree() {
    hopUi.refreshTree();
  }
}
