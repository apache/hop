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

package org.apache.hop.ui.hopui;

import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.LastUsedFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.job.entries.missing.MissingEntryDialog;
import org.w3c.dom.Node;

import java.util.Date;
import java.util.Locale;

public class JobFileListener implements FileListener {

  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  public boolean open( Node jobNode, String fname, boolean importfile ) {
    HopUi hopUi = HopUi.getInstance();
    try {
      // Call extension point(s) before the file has been opened
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.JobBeforeOpen.id, fname );

      JobMeta jobMeta = new JobMeta();
      jobMeta.loadXML( jobNode, fname, hopUi.getMetaStore(), false );
      if ( jobMeta.hasMissingPlugins() ) {
        MissingEntryDialog missingDialog = new MissingEntryDialog( hopUi.getShell(), jobMeta.getMissingEntries() );
        if ( missingDialog.open() == null ) {
          return true;
        }
      }
      jobMeta.setMetaStore( hopUi.getMetaStore() );
      hopUi.setJobMetaVariables( jobMeta );
      hopUi.getProperties().addLastFile( LastUsedFile.FILE_TYPE_JOB, fname, new Date() );
      hopUi.addMenuLast();

      // If we are importing into a repository we need to fix 
      // up the references to other jobs and transformations
      // if any exist.
      if ( !importfile ) {
        jobMeta.clearChanged();
      }

      jobMeta.setFilename( fname );
      hopUi.delegates.jobs.addJobGraph( jobMeta );

      // Call extension point(s) now that the file has been opened
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.JobAfterOpen.id, jobMeta );

      hopUi.refreshTree();
      HopUiPerspectiveManager.getInstance().activatePerspective( MainHopUiPerspective.class );
      return true;
    } catch ( HopException e ) {
      new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.ErrorOpening.Title" ), BaseMessages
        .getString( PKG, "Spoon.Dialog.ErrorOpening.Message" )
        + fname, e );
    }
    return false;
  }

  public boolean save( EngineMetaInterface meta, String fname, boolean export ) {
    HopUi hopUi = HopUi.getInstance();

    EngineMetaInterface lmeta;
    if ( export ) {
      lmeta = (JobMeta) ( (JobMeta) meta ).realClone( false );
    } else {
      lmeta = meta;
    }

    try {
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.JobBeforeSave.id, lmeta );
    } catch ( HopException e ) {
      // fails gracefully
    }

    boolean saveStatus = hopUi.saveMeta( lmeta, fname );

    if ( saveStatus ) {
      try {
        ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.JobAfterSave.id, lmeta );
      } catch ( HopException e ) {
        // fails gracefully
      }
    }

    return saveStatus;
  }

  public void syncMetaName( EngineMetaInterface meta, String name ) {
    ( (JobMeta) meta ).setName( name );
  }

  public boolean accepts( String fileName ) {
    if ( fileName == null || fileName.indexOf( '.' ) == -1 ) {
      return false;
    }
    String extension = fileName.substring( fileName.lastIndexOf( '.' ) + 1 );
    return extension.equals( "kjb" );
  }

  public boolean acceptsXml( String nodeName ) {
    return "job".equals( nodeName );
  }

  public String[] getFileTypeDisplayNames( Locale locale ) {
    return new String[] { "Jobs", "XML" };
  }

  public String[] getSupportedExtensions() {
    return new String[] { "kjb", "xml" };
  }

  public String getRootNodeName() {
    return "job";
  }

}
