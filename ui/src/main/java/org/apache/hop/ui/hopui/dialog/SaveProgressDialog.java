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

package org.apache.hop.ui.hopui.dialog;

import java.lang.reflect.InvocationTargetException;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.Repository;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.trans.dialog.TransDialog;

/**
 * Takes care of displaying a dialog that will handle the wait while saving a transformation...
 *
 * @author Matt
 * @since 13-mrt-2005
 */
public class SaveProgressDialog {
  private static Class<?> PKG = TransDialog.class; // for i18n purposes, needed by Translator2!!

  private Shell shell;
  private Repository rep;
  private EngineMetaInterface meta;

  private String versionComment;

  /**
   * Creates a new dialog that will handle the wait while saving a transformation...
   */
  public SaveProgressDialog( Shell shell, Repository rep, EngineMetaInterface meta, String versionComment ) {
    this.shell = shell;
    this.rep = rep;
    this.meta = meta;
    this.versionComment = versionComment;
  }

  public boolean open() {
    boolean retval = true;

    IRunnableWithProgress op = new IRunnableWithProgress() {
      public void run( IProgressMonitor monitor ) throws InvocationTargetException, InterruptedException {
        try {
          rep.save( meta, versionComment, new ProgressMonitorAdapter( monitor ) );
        } catch ( HopException e ) {
          throw new InvocationTargetException( e, BaseMessages.getString(
            PKG, "TransSaveProgressDialog.Exception.ErrorSavingTransformation" )
            + e.toString() );
        }
      }
    };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      MessageDialog errorDialog =
        new MessageDialog( shell, BaseMessages.getString( PKG, "TransSaveProgressDialog.UnableToSave.DialogTitle" ), null,
          BaseMessages.getString( PKG, "TransSaveProgressDialog.UnableToSave.DialogMessage" ), MessageDialog.ERROR,
          new String[] { BaseMessages.getString( PKG, "TransSaveProgressDialog.UnableToSave.Close" ) }, 0 );
      errorDialog.open();
      retval = false;
    } catch ( InterruptedException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "TransSaveProgressDialog.ErrorSavingTransformation.DialogTitle" ),
        BaseMessages.getString( PKG, "TransSaveProgressDialog.ErrorSavingTransformation.DialogMessage" ), e );
      retval = false;
    }

    return retval;
  }
}
