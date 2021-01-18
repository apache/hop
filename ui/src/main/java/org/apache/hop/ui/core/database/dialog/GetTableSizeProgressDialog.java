/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.database.dialog;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.InvocationTargetException;

/**
 * Takes care of displaying a dialog that will handle the wait while we're getting the number of rows for a certain
 * table in a database.
 *
 * @author Matt
 * @since 12-may-2005
 */
public class GetTableSizeProgressDialog {
  private static final Class<?> PKG = GetTableSizeProgressDialog.class; // For Translator

  private Shell shell;
  private IVariables variables;
  private DatabaseMeta databaseMeta;
  private String tableName;
  private Long size;

  private Database db;

  /**
   * Creates a new dialog that will handle the wait while we're doing the hard work.
   */
  public GetTableSizeProgressDialog( Shell shell, IVariables variables, DatabaseMeta databaseMeta, String tableName ) {
    this( shell, variables, databaseMeta, tableName, null );
  }

  public GetTableSizeProgressDialog( Shell shell, IVariables variables, DatabaseMeta databaseMeta, String tableName, String schemaName ) {
    this.shell = shell;
    this.databaseMeta = databaseMeta;
    this.tableName = databaseMeta.getQuotedSchemaTableCombination( variables, schemaName, tableName );
  }

  public Long open() {
    IRunnableWithProgress op = monitor -> {
      db = new Database( HopGui.getInstance().getLoggingObject(), variables, databaseMeta );
      try {
        db.connect();

        String sql = databaseMeta.getIDatabase().getSelectCountStatement( tableName );
        RowMetaAndData row = db.getOneRow( sql );
        size = row.getRowMeta().getInteger( row.getData(), 0 );

        if ( monitor.isCanceled() ) {
          throw new InvocationTargetException( new Exception( "This operation was cancelled!" ) );
        }

      } catch ( HopException e ) {
        throw new InvocationTargetException( e, "Couldn't get a result because of an error :" + e.toString() );
      } finally {
        db.disconnect();
      }
    };

    try {
      final ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      // Run something in the background to cancel active database queries, forcibly if needed!
      Runnable run = () -> {
        IProgressMonitor monitor = pmd.getProgressMonitor();
        while ( pmd.getShell() == null || ( !pmd.getShell().isDisposed() && !monitor.isCanceled() ) ) {
          try {
            Thread.sleep( 100 );
          } catch ( InterruptedException e ) {
            // Ignore
          }
        }

        if ( monitor.isCanceled() ) { // Disconnect and see what happens!

          try {
            db.cancelQuery();
          } catch ( Exception e ) {
            // Ignore
          }
        }
      };
      // Start the cancel tracker in the background!
      new Thread( run ).start();

      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      showErrorDialog( e );
      return null;
    } catch ( InterruptedException e ) {
      showErrorDialog( e );
      return null;
    }

    return size;
  }

  /**
   * Showing an error dialog
   *
   * @param e
   */
  private void showErrorDialog( Exception e ) {
    new ErrorDialog( shell, BaseMessages.getString( PKG, "GetTableSizeProgressDialog.Error.Title" ), BaseMessages
      .getString( PKG, "GetTableSizeProgressDialog.Error.Message" ), e );
  }
}
