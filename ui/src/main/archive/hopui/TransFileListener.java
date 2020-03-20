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

package org.apache.hop.ui.hopui;

import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.LastUsedFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopMissingPluginsException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.trans.steps.missing.MissingTransDialog;
import org.w3c.dom.Node;

import java.util.Date;
import java.util.Locale;

public class TransFileListener implements FileListener {

  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  public boolean open( Node transNode, String fname, boolean importfile ) throws HopMissingPluginsException {
    final HopUi hopUi = HopUi.getInstance();
    final PropsUI props = PropsUI.getInstance();
    try {
      // Call extension point(s) before the file has been opened
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.TransBeforeOpen.id, fname );

      TransMeta transMeta = new TransMeta();
      transMeta.loadXML( transNode, fname, hopUi.getMetaStore(), true, new Variables() );

      if ( transMeta.hasMissingPlugins() ) {
        StepMeta stepMeta = transMeta.getStep( 0 );
        MissingTransDialog missingDialog =
          new MissingTransDialog( hopUi.getShell(), transMeta.getMissingTrans(), stepMeta.getStepMetaInterface(),
            transMeta, stepMeta.getName() );
        if ( missingDialog.open() == null ) {
          return true;
        }
      }
      transMeta.setMetaStore( hopUi.getMetaStore() );
      hopUi.setTransMetaVariables( transMeta );
      hopUi.getProperties().addLastFile( LastUsedFile.FILE_TYPE_TRANSFORMATION, fname, new Date() );
      hopUi.addMenuLast();

      // If we are importing we need to fix
      // up the references to other jobs and transformations
      // if any exist.
      if ( !importfile ) {
        transMeta.clearChanged();
      }

      transMeta.setFilename( fname );
      hopUi.addTransGraph( transMeta );

      // Call extension point(s) now that the file has been opened
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.TransAfterOpen.id, transMeta );

      HopUiPerspectiveManager.getInstance().activatePerspective( MainHopUiPerspective.class );
      hopUi.refreshTree();
      return true;

    } catch ( HopMissingPluginsException e ) {
      throw e;
    } catch ( HopException e ) {
      new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ErrorOpening.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.ErrorOpening.Message" )
        + fname, e );
    }
    return false;
  }

  public boolean save( EngineMetaInterface meta, String fname, boolean export ) {
    HopUi hopUi = HopUi.getInstance();
    EngineMetaInterface lmeta;
    if ( export ) {
      lmeta = (TransMeta) ( (TransMeta) meta ).realClone( false );
    } else {
      lmeta = meta;
    }

    try {
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.TransBeforeSave.id, lmeta );
    } catch ( HopException e ) {
      // fails gracefully
    }

    boolean saveStatus = hopUi.saveMeta( lmeta, fname );

    if ( saveStatus ) {
      try {
        ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.TransAfterSave.id, lmeta );
      } catch ( HopException e ) {
        // fails gracefully
      }
    }

    return saveStatus;
  }

  public void syncMetaName( EngineMetaInterface meta, String name ) {
    ( (TransMeta) meta ).setName( name );
  }

  public boolean accepts( String fileName ) {
    if ( fileName == null || fileName.indexOf( '.' ) == -1 ) {
      return false;
    }
    String extension = fileName.substring( fileName.lastIndexOf( '.' ) + 1 );
    return extension.equals( "ktr" );
  }

  public boolean acceptsXml( String nodeName ) {
    if ( "transformation".equals( nodeName ) ) {
      return true;
    }
    return false;
  }

  public String[] getFileTypeDisplayNames( Locale locale ) {
    return new String[] { "Transformations", "XML" };
  }

  public String getRootNodeName() {
    return "transformation";
  }

  public String[] getSupportedExtensions() {
    return new String[] { "ktr", "xml" };
  }

}
