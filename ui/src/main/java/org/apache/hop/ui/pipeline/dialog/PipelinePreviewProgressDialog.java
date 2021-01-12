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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.debug.IBreakPointListener;
import org.apache.hop.pipeline.debug.PipelineDebugMeta;
import org.apache.hop.pipeline.debug.TransformDebugMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes care of displaying a dialog that will handle the wait while previewing a pipeline...
 *
 * @author Matt
 * @since 13-jan-2006
 */
public class PipelinePreviewProgressDialog {
  private static final Class<?> PKG = PipelineDialog.class; // For Translator

  private Shell shell;
  private final IVariables variables;
  private PipelineMeta pipelineMeta;
  private String[] previewTransformNames;
  private int[] previewSize;
  private Pipeline pipeline;

  private boolean cancelled;
  private String loggingText;
  private PipelineDebugMeta pipelineDebugMeta;

  /**
   * Creates a new dialog that will handle the wait while previewing a pipeline...
   */
  public PipelinePreviewProgressDialog( Shell shell, IVariables variables, PipelineMeta pipelineMeta, String[] previewTransformNames, int[] previewSize ) {
    this.shell = shell;
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.previewTransformNames = previewTransformNames;
    this.previewSize = previewSize;

    cancelled = false;
  }

  public PipelineMeta open() {
    return open( true );
  }

  /**
   * Opens the progress dialog
   *
   * @param showErrorDialogs dictates whether error dialogs should be shown when errors occur - can be set to false
   *                         to let the caller control error dialogs instead.
   * @return a {@link PipelineMeta}
   */
  public PipelineMeta open( final boolean showErrorDialogs ) {
    IRunnableWithProgress op = monitor -> doPreview( monitor, showErrorDialogs );

    try {
      final ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );

      // Run something in the background to cancel active database queries, forecably if needed!
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
            pipeline.stopAll();
          } catch ( Exception e ) { /* Ignore */
          }
        }
      };

      // Start the cancel tracker in the background!
      new Thread( run ).start();

      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      if ( showErrorDialogs ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "PipelinePreviewProgressDialog.ErrorLoadingPipeline.DialogTitle" ),
          BaseMessages.getString( PKG, "PipelinePreviewProgressDialog.ErrorLoadingPipeline.DialogMessage" ), e );
      }
      pipelineMeta = null;
    } catch ( InterruptedException e ) {
      if ( showErrorDialogs ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "PipelinePreviewProgressDialog.ErrorLoadingPipeline.DialogTitle" ),
          BaseMessages.getString( PKG, "PipelinePreviewProgressDialog.ErrorLoadingPipeline.DialogMessage" ), e );
      }
      pipelineMeta = null;
    }

    return pipelineMeta;
  }

  private void doPreview( final IProgressMonitor progressMonitor, final boolean showErrorDialogs ) {
    progressMonitor.beginTask( BaseMessages.getString( PKG, "PipelinePreviewProgressDialog.Monitor.BeginTask.Title" ), 100 );

    // This pipeline is ready to run in preview!
    //
    pipeline = new LocalPipelineEngine( pipelineMeta, variables, HopGui.getInstance().getLoggingObject() );
    pipeline.setPreview( true );

    // Prepare the execution...
    //
    try {
      pipeline.prepareExecution();
    } catch ( final HopException e ) {
      if ( showErrorDialogs ) {
        shell.getDisplay().asyncExec( () -> new ErrorDialog( shell,
          BaseMessages.getString( PKG, "System.Dialog.Error.Title" ),
          BaseMessages.getString( PKG, "PipelinePreviewProgressDialog.Exception.ErrorPreparingPipeline" ), e ) );
      }

      // It makes no sense to continue, so just stop running...
      //
      return;
    }

    // Add the preview / debugging information...
    //
    pipelineDebugMeta = new PipelineDebugMeta( pipelineMeta );
    for ( int i = 0; i < previewTransformNames.length; i++ ) {
      TransformMeta transformMeta = pipelineMeta.findTransform( previewTransformNames[ i ] );
      TransformDebugMeta transformDebugMeta = new TransformDebugMeta( transformMeta );
      transformDebugMeta.setReadingFirstRows( true );
      transformDebugMeta.setRowCount( previewSize[ i ] );
      pipelineDebugMeta.getTransformDebugMetaMap().put( transformMeta, transformDebugMeta );
    }

    int previousPct = 0;
    final List<String> previewComplete = new ArrayList<>();
    // We add a break-point that is called every time we have a transform with a full preview row buffer
    // That makes it easy and fast to see if we have all the rows we need
    //
    pipelineDebugMeta.addBreakPointListers( ( pipelineDebugMeta, transformDebugMeta, rowBufferMeta, rowBuffer ) -> {
      String transformName = transformDebugMeta.getTransformMeta().getName();
      previewComplete.add( transformName );
      progressMonitor.subTask( BaseMessages.getString(
        PKG, "PipelinePreviewProgressDialog.SubTask.TransformPreviewFinished", transformName ) );
    } );
    // set the appropriate listeners on the pipeline...
    //
    pipelineDebugMeta.addRowListenersToPipeline( pipeline );

    // Fire off the transform threads... start running!
    //
    try {
      pipeline.startThreads();
    } catch ( final HopException e ) {
      shell.getDisplay().asyncExec( () -> new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
        .getString( PKG, "PipelinePreviewProgressDialog.Exception.ErrorPreparingPipeline" ), e ) );

      // It makes no sense to continue, so just stop running...
      //
      return;
    }

    while ( previewComplete.size() < previewTransformNames.length
      && !pipeline.isFinished() && !progressMonitor.isCanceled() ) {

      // How many rows are done?
      int nrDone = 0;
      int nrTotal = 0;
      for ( TransformDebugMeta transformDebugMeta : pipelineDebugMeta.getTransformDebugMetaMap().values() ) {
        nrDone += transformDebugMeta.getRowBuffer().size();
        nrTotal += transformDebugMeta.getRowCount();
      }

      int pct = 100 * nrDone / nrTotal;

      int worked = pct - previousPct;

      if ( worked > 0 ) {
        progressMonitor.worked( worked );
      }
      previousPct = pct;

      // Change the percentage...
      try {
        Thread.sleep( 500 );
      } catch ( InterruptedException e ) {
        // Ignore errors
      }

      if ( progressMonitor.isCanceled() ) {
        cancelled = true;
        pipeline.stopAll();
      }
    }

    pipeline.stopAll();

    // Capture preview activity to a String:
    loggingText =
      HopLogStore.getAppender().getBuffer( pipeline.getLogChannel().getLogChannelId(), true ).toString();

    progressMonitor.done();
  }

  /**
   * @param transformName the name of the transform to get the preview rows for
   * @return A list of rows as the result of the preview run.
   */
  public List<Object[]> getPreviewRows( String transformName ) {
    if ( pipelineDebugMeta == null ) {
      return null;
    }

    for ( TransformMeta transformMeta : pipelineDebugMeta.getTransformDebugMetaMap().keySet() ) {
      if ( transformMeta.getName().equals( transformName ) ) {
        TransformDebugMeta transformDebugMeta = pipelineDebugMeta.getTransformDebugMetaMap().get( transformMeta );
        return transformDebugMeta.getRowBuffer();
      }
    }
    return null;
  }

  /**
   * @param transformName the name of the transform to get the preview rows for
   * @return A description of the row (metadata)
   */
  public IRowMeta getPreviewRowsMeta( String transformName ) {
    if ( pipelineDebugMeta == null ) {
      return null;
    }

    for ( TransformMeta transformMeta : pipelineDebugMeta.getTransformDebugMetaMap().keySet() ) {
      if ( transformMeta.getName().equals( transformName ) ) {
        TransformDebugMeta transformDebugMeta = pipelineDebugMeta.getTransformDebugMetaMap().get( transformMeta );
        return transformDebugMeta.getRowBufferMeta();
      }
    }
    return null;
  }

  /**
   * @return true is the preview was canceled by the user
   */
  public boolean isCancelled() {
    return cancelled;
  }

  /**
   * @return The logging text from the latest preview run
   */
  public String getLoggingText() {
    return loggingText;
  }

  /**
   * @return The pipeline object that executed the preview PipelineMeta
   */
  public Pipeline getPipeline() {
    return pipeline;
  }

  /**
   * @return the pipelineDebugMeta
   */
  public PipelineDebugMeta getPipelineDebugMeta() {
    return pipelineDebugMeta;
  }
}
