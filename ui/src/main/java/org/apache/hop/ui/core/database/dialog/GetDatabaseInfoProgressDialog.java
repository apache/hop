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

import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaInformation;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.InvocationTargetException;

/**
 * Takes care of displaying a dialog that will handle the wait while we're finding out what tables, views etc we can
 * reach in the database.
 *
 * @author Matt
 * @since 07-apr-2005
 */
public class GetDatabaseInfoProgressDialog {
  private static final Class<?> PKG = GetDatabaseInfoProgressDialog.class; // For Translator

  private Shell shell;
  private final IVariables variables;
  private DatabaseMeta databaseMeta;

  /**
   * Creates a new dialog that will handle the wait while we're finding out what tables, views etc we can reach in the
   * database.
   */
  public GetDatabaseInfoProgressDialog( Shell shell, IVariables variables, DatabaseMeta databaseMeta ) {
    this.shell = shell;
    this.variables = variables;
    this.databaseMeta = databaseMeta;
  }

  public DatabaseMetaInformation open() {
    final DatabaseMetaInformation dmi = new DatabaseMetaInformation( variables, databaseMeta );
    IRunnableWithProgress op = monitor -> {
      try {
        dmi.getData( HopGui.getInstance().getLoggingObject(), new ProgressMonitorAdapter( monitor ) );
      } catch ( Exception e ) {
        throw new InvocationTargetException( e, BaseMessages.getString(
          PKG, "GetDatabaseInfoProgressDialog.Error.GettingInfoTable", e.toString() ) );
      }
    };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );

      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      showErrorDialog( e );
      return null;
    } catch ( InterruptedException e ) {
      showErrorDialog( e );
      return null;
    }

    return dmi;
  }

  /**
   * Showing an error dialog
   *
   * @param e
   */
  private void showErrorDialog( Exception e ) {
    new ErrorDialog(
      shell, BaseMessages.getString( PKG, "GetDatabaseInfoProgressDialog.Error.Title" ), BaseMessages.getString(
      PKG, "GetDatabaseInfoProgressDialog.Error.Message" ), e );
  }
}
