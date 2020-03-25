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

package org.apache.hop.ui.hopgui.file.trans.delegates;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.debug.StepDebugMeta;
import org.apache.hop.trans.debug.TransDebugMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.trans.HopGuiTransGraph;
import org.apache.hop.ui.trans.debug.TransDebugDialog;
import org.apache.hop.ui.trans.dialog.TransExecutionConfigurationDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HopGuiTransRunDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator2!!

  private HopGuiTransGraph transGraph;
  private HopGui hopUi;

  private TransExecutionConfiguration transExecutionConfiguration;
  private TransExecutionConfiguration transPreviewExecutionConfiguration;
  private TransExecutionConfiguration transDebugExecutionConfiguration;

  /**
   * This contains a map between the name of a transformation and the TransMeta object. If the transformation has no
   * name it will be mapped under a number [1], [2] etc.
   */
  private List<TransMeta> transformationMap;

  /**
   * Remember the debugging configuration per transformation
   */
  private Map<TransMeta, TransDebugMeta> transDebugMetaMap;

  /**
   * Remember the preview configuration per transformation
   */
  private Map<TransMeta, TransDebugMeta> transPreviewMetaMap;


  /**
   * @param hopUi
   */
  public HopGuiTransRunDelegate( HopGui hopUi, HopGuiTransGraph transGraph ) {
    this.hopUi = hopUi;
    this.transGraph = transGraph;

    transExecutionConfiguration = new TransExecutionConfiguration();
    transExecutionConfiguration.setGatheringMetrics( true );
    transPreviewExecutionConfiguration = new TransExecutionConfiguration();
    transPreviewExecutionConfiguration.setGatheringMetrics( true );
    transDebugExecutionConfiguration = new TransExecutionConfiguration();
    transDebugExecutionConfiguration.setGatheringMetrics( true );

    transformationMap = new ArrayList<>();
    transDebugMetaMap = new HashMap<>();
    transPreviewMetaMap = new HashMap<>();
  }

  public TransExecutionConfiguration executeTransformation( final LogChannelInterface log, final TransMeta transMeta, final boolean local, final boolean remote,
                                                            final boolean preview, final boolean debug, final boolean safe, LogLevel logLevel ) throws HopException {

    if ( transMeta == null ) {
      return null;
    }

    // See if we need to ask for debugging information...
    //
    TransDebugMeta transDebugMeta = null;
    TransExecutionConfiguration executionConfiguration = null;

    if ( preview ) {
      executionConfiguration = getTransPreviewExecutionConfiguration();
    } else if ( debug ) {
      executionConfiguration = getTransDebugExecutionConfiguration();
    } else {
      executionConfiguration = getTransExecutionConfiguration();
    }

    // Set defaults so the run configuration can set it up correctly
    executionConfiguration.setExecutingLocally( true );
    executionConfiguration.setExecutingRemotely( false );

    // Set MetaStore and safe mode information in both the exec config and the metadata
    //
    transMeta.setMetaStore( hopUi.getMetaStore() );
    executionConfiguration.setSafeModeEnabled( safe );

    if ( debug ) {
      // See if we have debugging information stored somewhere?
      //
      transDebugMeta = transDebugMetaMap.get( transMeta );
      if ( transDebugMeta == null ) {
        transDebugMeta = new TransDebugMeta( transMeta );
        transDebugMetaMap.put( transMeta, transDebugMeta );
      }

      // Set the default number of rows to retrieve on all selected steps...
      //
      List<StepMeta> selectedSteps = transMeta.getSelectedSteps();
      if ( selectedSteps != null && selectedSteps.size() > 0 ) {
        transDebugMeta.getStepDebugMetaMap().clear();
        for ( StepMeta stepMeta : transMeta.getSelectedSteps() ) {
          StepDebugMeta stepDebugMeta = new StepDebugMeta( stepMeta );
          stepDebugMeta.setRowCount( PropsUI.getInstance().getDefaultPreviewSize() );
          stepDebugMeta.setPausingOnBreakPoint( true );
          stepDebugMeta.setReadingFirstRows( false );
          transDebugMeta.getStepDebugMetaMap().put( stepMeta, stepDebugMeta );
        }
      }

    } else if ( preview ) {
      // See if we have preview information stored somewhere?
      //
      transDebugMeta = transPreviewMetaMap.get( transMeta );
      if ( transDebugMeta == null ) {
        transDebugMeta = new TransDebugMeta( transMeta );

        transPreviewMetaMap.put( transMeta, transDebugMeta );
      }

      // Set the default number of preview rows on all selected steps...
      //
      List<StepMeta> selectedSteps = transMeta.getSelectedSteps();
      if ( selectedSteps != null && selectedSteps.size() > 0 ) {
        transDebugMeta.getStepDebugMetaMap().clear();
        for ( StepMeta stepMeta : transMeta.getSelectedSteps() ) {
          StepDebugMeta stepDebugMeta = new StepDebugMeta( stepMeta );
          stepDebugMeta.setRowCount( PropsUI.getInstance().getDefaultPreviewSize() );
          stepDebugMeta.setPausingOnBreakPoint( false );
          stepDebugMeta.setReadingFirstRows( true );
          transDebugMeta.getStepDebugMetaMap().put( stepMeta, stepDebugMeta );
        }
      }
    }

    int debugAnswer = TransDebugDialog.DEBUG_CONFIG;

    if ( debug || preview ) {
      TransDebugDialog transDebugDialog = new TransDebugDialog( hopUi.getShell(), transDebugMeta );
      debugAnswer = transDebugDialog.open();
      if ( debugAnswer != TransDebugDialog.DEBUG_CANCEL ) {
        executionConfiguration.setExecutingLocally( true );
        executionConfiguration.setExecutingRemotely( false );
      } else {
        // If we cancel the debug dialog, we don't go further with the execution either.
        //
        return null;
      }
    }

    Map<String, String> variableMap = new HashMap<>();
    variableMap.putAll( executionConfiguration.getVariables() ); // the default

    executionConfiguration.setVariables( variableMap );
    executionConfiguration.getUsedVariables( transMeta );

    executionConfiguration.setLogLevel( logLevel );

    boolean execConfigAnswer = true;

    if ( debugAnswer == TransDebugDialog.DEBUG_CONFIG && transMeta.isShowDialog() ) {
      TransExecutionConfigurationDialog dialog =
        new TransExecutionConfigurationDialog( hopUi.getShell(), executionConfiguration, transMeta );
      execConfigAnswer = dialog.open();
    }

    if ( execConfigAnswer ) {
      transGraph.transGridDelegate.addTransGrid();
      transGraph.transLogDelegate.addTransLog();
      transGraph.transPreviewDelegate.addTransPreview();
      transGraph.transMetricsDelegate.addTransMetrics();
      transGraph.transPerfDelegate.addTransPerf();
      transGraph.extraViewTabFolder.setSelection( 0 );

      // Set the named parameters
      Map<String, String> paramMap = executionConfiguration.getParams();
      for ( String key : paramMap.keySet() ) {
        transMeta.setParameterValue( key, Const.NVL( paramMap.get( key ), "" ) );
      }
      transMeta.activateParameters();

      // Set the log level
      //
      if ( executionConfiguration.getLogLevel() != null ) {
        transMeta.setLogLevel( executionConfiguration.getLogLevel() );
      }

      // Set the run options
      transMeta.setClearingLog( executionConfiguration.isClearingLog() );
      transMeta.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
      transMeta.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );

      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransMetaExecutionStart.id, transMeta );
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransExecutionConfiguration.id,
        executionConfiguration );

      try {
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransBeforeStart.id, new Object[] { executionConfiguration, transMeta, transMeta } );
      } catch ( HopException e ) {
        log.logError( e.getMessage(), transMeta.getFilename() );
        return null;
      }

      if ( !executionConfiguration.isExecutingLocally() && !executionConfiguration.isExecutingRemotely() ) {
        if ( transMeta.hasChanged() ) {
          transGraph.showSaveFileMessage();
        }
      }

      // Verify if there is at least one step specified to debug or preview...
      //
      if ( debug || preview ) {
        if ( transDebugMeta.getNrOfUsedSteps() == 0 ) {
          MessageBox box = new MessageBox( hopUi.getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO );
          box.setText( BaseMessages.getString( PKG, "HopGui.Dialog.Warning.NoPreviewOrDebugSteps.Title" ) );
          box.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.Warning.NoPreviewOrDebugSteps.Message" ) );
          int answer = box.open();
          if ( answer != SWT.YES ) {
            return null;
          }
        }
      }

      // Is this a local execution?
      //
      if ( executionConfiguration.isExecutingLocally() ) {
        if ( debug || preview ) {
          transGraph.debug( executionConfiguration, transDebugMeta );
        } else {
          transGraph.start( executionConfiguration );
        }

        // Are we executing remotely?
        //
      } else if ( executionConfiguration.isExecutingRemotely() ) {
        transGraph.handleTransMetaChanges( transMeta );
        if ( transMeta.hasChanged() ) {
          showSaveTransformationBeforeRunningDialog( hopUi.getShell() );
        } else if ( executionConfiguration.getRemoteServer() != null ) {
          String carteObjectId =
            Trans.sendToSlaveServer( transMeta, executionConfiguration, hopUi.getMetaStore() );
          monitorRemoteTrans( transMeta, carteObjectId, executionConfiguration.getRemoteServer() );

          // TODO: add slave server monitor in different perspective
          // Also: make remote execution just like local execution through execution configurations and plugable engines.
          //hopUi.delegates.slaves.addHopGuiSlave( executionConfiguration.getRemoteServer() );

        } else {
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.NoRemoteServerSpecified.Message" ) );
          mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.NoRemoteServerSpecified.Title" ) );
          mb.open();
        }
      }
    }
    return executionConfiguration;
  }

  private static void showSaveTransformationBeforeRunningDialog( Shell shell ) {
    MessageBox m = new MessageBox( shell, SWT.OK | SWT.ICON_WARNING );
    m.setText( BaseMessages.getString( PKG, "TransLog.Dialog.SaveTransformationBeforeRunning.Title" ) );
    m.setMessage( BaseMessages.getString( PKG, "TransLog.Dialog.SaveTransformationBeforeRunning.Message" ) );
    m.open();
  }

  private void monitorRemoteTrans( final TransMeta transMeta, final String carteObjectId,
                                   final SlaveServer remoteSlaveServer ) {
    // There is a transformation running in the background. When it finishes, clean it up and log the result on the
    // console.
    // Launch in a separate thread to prevent GUI blocking...
    //
    Thread thread = new Thread( new Runnable() {
      public void run() {
        remoteSlaveServer.monitorRemoteTransformation( hopUi.getLog(), carteObjectId, transMeta.toString() );
      }
    } );

    thread.setName( "Monitor remote transformation '" + transMeta.getName() + "', carte object id=" + carteObjectId
      + ", slave server: " + remoteSlaveServer.getName() );
    thread.start();
  }


  /**
   * Gets transGraph
   *
   * @return value of transGraph
   */
  public HopGuiTransGraph getTransGraph() {
    return transGraph;
  }

  /**
   * @param transGraph The transGraph to set
   */
  public void setTransGraph( HopGuiTransGraph transGraph ) {
    this.transGraph = transGraph;
  }

  /**
   * Gets hopUi
   *
   * @return value of hopUi
   */
  public HopGui getHopUi() {
    return hopUi;
  }

  /**
   * @param hopUi The hopUi to set
   */
  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }

  /**
   * Gets transExecutionConfiguration
   *
   * @return value of transExecutionConfiguration
   */
  public TransExecutionConfiguration getTransExecutionConfiguration() {
    return transExecutionConfiguration;
  }

  /**
   * @param transExecutionConfiguration The transExecutionConfiguration to set
   */
  public void setTransExecutionConfiguration( TransExecutionConfiguration transExecutionConfiguration ) {
    this.transExecutionConfiguration = transExecutionConfiguration;
  }

  /**
   * Gets transPreviewExecutionConfiguration
   *
   * @return value of transPreviewExecutionConfiguration
   */
  public TransExecutionConfiguration getTransPreviewExecutionConfiguration() {
    return transPreviewExecutionConfiguration;
  }

  /**
   * @param transPreviewExecutionConfiguration The transPreviewExecutionConfiguration to set
   */
  public void setTransPreviewExecutionConfiguration( TransExecutionConfiguration transPreviewExecutionConfiguration ) {
    this.transPreviewExecutionConfiguration = transPreviewExecutionConfiguration;
  }

  /**
   * Gets transDebugExecutionConfiguration
   *
   * @return value of transDebugExecutionConfiguration
   */
  public TransExecutionConfiguration getTransDebugExecutionConfiguration() {
    return transDebugExecutionConfiguration;
  }

  /**
   * @param transDebugExecutionConfiguration The transDebugExecutionConfiguration to set
   */
  public void setTransDebugExecutionConfiguration( TransExecutionConfiguration transDebugExecutionConfiguration ) {
    this.transDebugExecutionConfiguration = transDebugExecutionConfiguration;
  }

  /**
   * Gets transformationMap
   *
   * @return value of transformationMap
   */
  public List<TransMeta> getTransformationMap() {
    return transformationMap;
  }

  /**
   * @param transformationMap The transformationMap to set
   */
  public void setTransformationMap( List<TransMeta> transformationMap ) {
    this.transformationMap = transformationMap;
  }

  /**
   * Gets transDebugMetaMap
   *
   * @return value of transDebugMetaMap
   */
  public Map<TransMeta, TransDebugMeta> getTransDebugMetaMap() {
    return transDebugMetaMap;
  }

  /**
   * @param transDebugMetaMap The transDebugMetaMap to set
   */
  public void setTransDebugMetaMap( Map<TransMeta, TransDebugMeta> transDebugMetaMap ) {
    this.transDebugMetaMap = transDebugMetaMap;
  }

  /**
   * Gets transPreviewMetaMap
   *
   * @return value of transPreviewMetaMap
   */
  public Map<TransMeta, TransDebugMeta> getTransPreviewMetaMap() {
    return transPreviewMetaMap;
  }

  /**
   * @param transPreviewMetaMap The transPreviewMetaMap to set
   */
  public void setTransPreviewMetaMap( Map<TransMeta, TransDebugMeta> transPreviewMetaMap ) {
    this.transPreviewMetaMap = transPreviewMetaMap;
  }
}
