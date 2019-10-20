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

package org.apache.hop.ui.repository;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.repository.HopRepositoryLostException;
import org.apache.hop.repository.Repository;
import org.apache.hop.repository.RepositoryOperation;
import org.apache.hop.repository.RepositorySecurityProvider;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.spoon.Spoon;

public class RepositorySecurityUI {

  /**
   * Verify a repository operation, show an error dialog if needed.
   *
   * @param shell
   *     The parent frame to display error dialog
   * @param repository
   *     The repository meta object
   * @param displayError
   *     If true, then display an error dialog if there is a security error
   * @param operations
   *     the operations you want to perform with the supplied user.
   * @return true if there is an error, false if all is OK.
   */
  public static boolean verifyOperations( Shell shell, Repository repository, boolean displayError,
      RepositoryOperation... operations ) {
    String operationsDesc = "[";

    if ( displayError ) {
      for ( RepositoryOperation operation : operations ) {
        if ( operationsDesc.length() > 1 ) {
          operationsDesc += ", ";
        }
        operationsDesc += operation.getDescription();
      }
      operationsDesc += "]";
    }

    try {
      if ( repository != null ) {
        repository.getSecurityProvider().validateAction( operations );
      }
      // always ok if there is no repository
      return false;
    } catch ( HopRepositoryLostException krle ) {
      Spoon.getInstance().handleRepositoryLost( krle );
    } catch ( HopException e ) {
      HopRepositoryLostException krle = HopRepositoryLostException.lookupStackStrace( e );
      if ( krle != null ) {
        Spoon.getInstance().handleRepositoryLost( krle );
      } else if ( displayError == true ) {
        new ErrorDialog( shell, "Security error",
            "There was a security error performing operations:" + Const.CR + operationsDesc, e );
      }
    }
    return true;
  }

  /**
   * Verify a repository operation, show an error dialog if there is a security error.
   *
   * @param shell
   *     The parent frame to display error dialog
   * @param repository
   *     The repository meta object
   * @param operations
   *     the operations you want to perform with the supplied user.
   * @return true if there is an error, false if all is OK.
   */
  public static boolean verifyOperations( Shell shell, Repository repository, RepositoryOperation... operations ) {
    return verifyOperations( shell, repository, true, operations );
  }

  public static String getVersionComment( Shell shell, Repository repository, String operationDescription,
      String fullPath, boolean forceEntry ) {
    //forceEntry is used to force the comment prompt when multiple files will be affected.  It 
    //removes a web service call per file.
    if ( repository == null ) {
      return null;
    }

    RepositorySecurityProvider provider = repository.getSecurityProvider();
    if ( forceEntry || provider.allowsVersionComments( fullPath ) ) {

      String explanation = "Enter a comment ";
      if ( provider.isVersionCommentMandatory() ) {
        explanation += "(Mandatory) : ";
      } else {
        explanation += ": ";
      }
      String versionComment = "Checked in";

      EnterStringDialog dialog = new EnterStringDialog( shell, versionComment, "Enter comment", explanation );
      dialog.setManditory( provider.isVersionCommentMandatory() );
      versionComment = dialog.open();

      return versionComment;
    }
    return null;
  }

  /**
   * @param shell
   *          the parent shell.
   * @return true if we need to retry, false if we need to cancel the operation.
   */
  public static boolean showVersionCommentMandatoryDialog( Shell shell ) {
    MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
    box.setMessage( "Version comments are mandatory for this repository."
      + Const.CR + "Do you want to enter a comment?" );
    box.setText( "Version comments are mandatory!" );
    return box.open() == SWT.YES;
  }

}
