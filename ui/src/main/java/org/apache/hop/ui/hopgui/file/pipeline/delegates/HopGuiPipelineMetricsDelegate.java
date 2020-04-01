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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.MetricsPainter;
import org.apache.hop.core.logging.MetricsPainter.MetricsDrawArea;
import org.apache.hop.core.metrics.MetricsDuration;
import org.apache.hop.core.metrics.MetricsUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.shared.SWTGC;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ControlAdapter;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class HopGuiPipelineMetricsDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  // private static final LogWriter log = LogWriter.getInstance();

  private HopGui hopUi;
  private HopGuiPipelineGraph pipelineGraph;

  private CTabItem pipelineMetricsTab;

  private Canvas canvas;
  private Image image;

  private ScrolledComposite sMetricsComposite;
  private Composite metricsComposite;
  private boolean emptyGraph;

  private List<MetricsDrawArea> drawAreas;

  /**
   * @param hopUi
   * @param pipelineGraph
   */
  public HopGuiPipelineMetricsDelegate( HopGui hopUi, HopGuiPipelineGraph pipelineGraph ) {
    this.hopUi = hopUi;
    this.pipelineGraph = pipelineGraph;
  }

  public void addPipelineMetrics() {
    // First, see if we need to add the extra view...
    //
    if ( pipelineGraph.extraViewComposite == null || pipelineGraph.extraViewComposite.isDisposed() ) {
      pipelineGraph.addExtraView();
    } else {
      if ( pipelineMetricsTab != null && !pipelineMetricsTab.isDisposed() ) {
        // just set this one active and get out...
        //
        pipelineGraph.extraViewTabFolder.setSelection( pipelineMetricsTab );
        return;
      }
    }

    // Add a pipelineMetricsTab : displays the metrics information in a graphical way...
    //
    pipelineMetricsTab = new CTabItem( pipelineGraph.extraViewTabFolder, SWT.NONE );
    pipelineMetricsTab.setImage( GUIResource.getInstance().getImageGantt() );
    pipelineMetricsTab.setText( BaseMessages.getString( PKG, "HopGui.PipelineGraph.MetricsTab.Name" ) );

    sMetricsComposite = new ScrolledComposite( pipelineGraph.extraViewTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    sMetricsComposite.setLayout( new FillLayout() );

    // Create a composite, slam everything on there like it was in the history tab.
    //
    metricsComposite = new Composite( sMetricsComposite, SWT.NONE );
    metricsComposite.setBackground( GUIResource.getInstance().getColorBackground() );
    metricsComposite.setLayout( new FormLayout() );

    hopUi.getProps().setLook( metricsComposite );

    setupContent();

    sMetricsComposite.setContent( metricsComposite );
    sMetricsComposite.setExpandHorizontal( true );
    sMetricsComposite.setExpandVertical( true );
    sMetricsComposite.setMinWidth( 800 );
    sMetricsComposite.setMinHeight( 400 );

    pipelineMetricsTab.setControl( sMetricsComposite );

    pipelineGraph.extraViewTabFolder.setSelection( pipelineMetricsTab );

    pipelineGraph.extraViewTabFolder.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent arg0 ) {
        layoutMetricsComposite();
        updateGraph();
      }
    } );
  }

  public void setupContent() {

    if ( metricsComposite.isDisposed() ) {
      return;
    }

    // Remove anything on the perf composite, like an empty page message
    //
    for ( Control control : metricsComposite.getChildren() ) {
      if ( !control.isDisposed() ) {
        control.dispose();
      }
    }

    emptyGraph = false;

    canvas = new Canvas( metricsComposite, SWT.NONE );
    hopUi.getProps().setLook( canvas );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( 0, 0 );
    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.top = new FormAttachment( 0, 0 );
    fdCanvas.bottom = new FormAttachment( 100, 0 );
    canvas.setLayoutData( fdCanvas );

    metricsComposite.addControlListener( new ControlAdapter() {
      public void controlResized( ControlEvent event ) {
        updateGraph();
      }
    } );

    metricsComposite.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent event ) {
        if ( image != null ) {
          image.dispose();
        }
      }
    } );

    canvas.addPaintListener( new PaintListener() {

      public void paintControl( final PaintEvent event ) {
        if ( pipelineGraph.pipeline != null && pipelineGraph.pipeline.isFinished() ) {
          refreshImage( event.gc );

          if ( image != null && !image.isDisposed() ) {
            event.gc.drawImage( image, 0, 0 );
          }
        } else {
          Rectangle bounds = canvas.getBounds();
          if ( bounds.width <= 0 || bounds.height <= 0 ) {
            return;
          }

          event.gc.setForeground( GUIResource.getInstance().getColorWhite() );
          event.gc.setBackground( GUIResource.getInstance().getColorWhite() );
          event.gc.fillRectangle( new Rectangle( 0, 0, bounds.width, bounds.height ) );
          event.gc.setForeground( GUIResource.getInstance().getColorBlack() );
          String metricsMessage =
            BaseMessages.getString( PKG, "PipelineMetricsDelegate.PipelineIsNotRunning.Message" );
          org.eclipse.swt.graphics.Point extent = event.gc.textExtent( metricsMessage );
          event.gc.drawText( metricsMessage, ( bounds.width - extent.x ) / 2, ( bounds.height - extent.y ) / 2 );
        }
      }
    } );

    // Refresh automatically every 5 seconds as well.
    //
    final Timer timer = new Timer( "HopGuiPipelineMetricsDelegate Timer" );
    timer.schedule( new TimerTask() {
      public void run() {
        updateGraph();
      }
    }, 0, 5000 );

    // When the tab is closed, we remove the update timer
    //
    pipelineMetricsTab.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent arg0 ) {
        timer.cancel();
      }
    } );

    // Show tool tips with details...
    //
    canvas.addMouseListener( new MouseAdapter() {
      @Override
      public void mouseDown( MouseEvent event ) {
        if ( drawAreas == null ) {
          return;
        }

        for ( int i = drawAreas.size() - 1; i >= 0; i-- ) {
          MetricsDrawArea drawArea = drawAreas.get( i );
          if ( drawArea.getArea().contains( event.x, event.y ) ) {
            MetricsDuration duration = drawArea.getDuration();
            if ( duration == null ) {
              continue;
            }
            LoggingObjectInterface loggingObject =
              LoggingRegistry.getInstance().getLoggingObject( duration.getLogChannelId() );
            if ( loggingObject == null ) {
              continue;
            }
          }
        }
      }
    } );

    /*
     * canvas.addControlListener(new ControlAdapter() {
     *
     * @Override public void controlResized(ControlEvent arg0) { lastRefreshTime=0; // force a refresh } });
     */
  }

  public void showMetricsView() {
    if ( pipelineMetricsTab == null || pipelineMetricsTab.isDisposed() ) {
      addPipelineMetrics();
    } else {
      pipelineMetricsTab.dispose();

      pipelineGraph.checkEmptyExtraView();
    }
  }

  public void updateGraph() {

    pipelineGraph.getDisplay().asyncExec( new Runnable() {
      public void run() {

        if ( metricsComposite != null
          && !metricsComposite.isDisposed() && canvas != null && !canvas.isDisposed() && pipelineMetricsTab != null
          && !pipelineMetricsTab.isDisposed() ) {
          if ( pipelineMetricsTab.isShowing() ) {
            canvas.redraw();
          }
        }
      }
    } );
  }

  private long lastRefreshTime;

  public void resetLastRefreshTime() {
    lastRefreshTime = 0L;
  }

  private void refreshImage( GC canvasGc ) {
    List<MetricsDuration> durations = MetricsUtil.getAllDurations( pipelineGraph.pipeline.getLogChannelId() );
    if ( Utils.isEmpty( durations ) ) {
      // In case of an empty durations or null there is nothing to draw
      return;
    }

    // Sort the metrics.
    Collections.sort( durations, new Comparator<MetricsDuration>() {
      @Override
      public int compare( MetricsDuration o1, MetricsDuration o2 ) {
        return o1.getDate().compareTo( o2.getDate() );
      }
    } );

    Rectangle bounds = canvas.getBounds();
    if ( bounds.width <= 0 || bounds.height <= 0 ) {
      return;
    }

    if ( pipelineGraph.pipeline == null ) {
      image = null;
      return;
    }

    // For performance reasons, only ever refresh this image at most every 5 seconds...
    //
    if ( image != null && ( System.currentTimeMillis() - lastRefreshTime ) < 5000 ) {
      return;
    }
    lastRefreshTime = System.currentTimeMillis();

    if ( image != null ) {
      image.dispose(); // prevent out of memory...
      image = null;
    }

    // Correct size of canvas.
    org.eclipse.swt.graphics.Point textExtent = canvasGc.textExtent( "AagKkiw" );
    int height = textExtent.y + 8;

    // Make the height larger if needed for clarify
    //
    bounds.height = Math.max( durations.size() * height, bounds.height );
    canvas.setSize( bounds.width, bounds.height );

    SWTGC gc = new SWTGC( Display.getCurrent(), new Point( bounds.width, bounds.height ), PropsUI.getInstance().getIconSize() );
    MetricsPainter painter = new MetricsPainter( gc, height );
    drawAreas = painter.paint( durations );
    image = (Image) gc.getImage();

    // refresh the scrolled composite
    //
    sMetricsComposite.setMinHeight( bounds.height );
    sMetricsComposite.setMinWidth( bounds.width );
    sMetricsComposite.layout( true, true );

    // Draw the image on the canvas...
    //
    canvas.redraw();

    // close shop on the SWT GC side.
    //
    gc.dispose();
  }

  /**
   * @return the pipeline Metrics Tab
   */
  public CTabItem getPipelineMetricsTab() {
    return pipelineMetricsTab;
  }

  /**
   * @return the emptyGraph
   */
  public boolean isEmptyGraph() {
    return emptyGraph;
  }

  public void layoutMetricsComposite() {
    if ( !metricsComposite.isDisposed() ) {
      metricsComposite.layout( true, true );
    }
  }

}
