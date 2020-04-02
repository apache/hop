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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.performance.PerformanceSnapShot;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.pipeline.dialog.PipelineDialog;
import org.apache.hop.ui.util.ImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ControlAdapter;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.category.DefaultCategoryDataset;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class HopGuiPipelinePerfDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  // private static final LogWriter log = LogWriter.getInstance();

  private static final int DATA_CHOICE_WRITTEN = 0;
  private static final int DATA_CHOICE_READ = 1;
  private static final int DATA_CHOICE_INPUT = 2;
  private static final int DATA_CHOICE_OUTPUT = 3;
  private static final int DATA_CHOICE_UPDATED = 4;
  private static final int DATA_CHOICE_REJECTED = 5;
  private static final int DATA_CHOICE_INPUT_BUFFER_SIZE = 6;
  private static final int DATA_CHOICE_OUTPUT_BUFFER_SIZE = 7;

  private static String[] dataChoices = new String[] {
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Written" ),
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Read" ),
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Input" ),
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Output" ),
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Updated" ),
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Rejected" ),
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.InputBufferSize" ),
    BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.OutputBufferSize" ), };

  private HopGui hopUi;
  private HopGuiPipelineGraph pipelineGraph;

  private CTabItem pipelinePerfTab;

  private Map<IEngineComponent, List<PerformanceSnapShot>> transformPerformanceSnapShots;
  private IEngineComponent[] transforms;
  private org.eclipse.swt.widgets.List transformsList;
  private Canvas canvas;
  private Image image;
  private long timeDifference;
  private String title;
  private org.eclipse.swt.widgets.List dataList;
  private Composite perfComposite;
  private boolean emptyGraph;

  /**
   * @param hopUi
   * @param pipelineGraph
   */
  public HopGuiPipelinePerfDelegate( HopGui hopUi, HopGuiPipelineGraph pipelineGraph ) {
    this.hopUi = hopUi;
    this.pipelineGraph = pipelineGraph;
  }

  public void addPipelinePerf() {
    // First, see if we need to add the extra view...
    //
    if ( pipelineGraph.extraViewComposite == null || pipelineGraph.extraViewComposite.isDisposed() ) {
      pipelineGraph.addExtraView();
    } else {
      if ( pipelinePerfTab != null && !pipelinePerfTab.isDisposed() ) {
        // just set this one active and get out...
        //
        pipelineGraph.extraViewTabFolder.setSelection( pipelinePerfTab );
        return;
      }
    }

    // Add a pipelineLogTab : display the logging...
    //
    pipelinePerfTab = new CTabItem( pipelineGraph.extraViewTabFolder, SWT.NONE );
    pipelinePerfTab.setImage( GUIResource.getInstance().getImageShowPerf() );
    pipelinePerfTab.setText( BaseMessages.getString( PKG, "HopGui.PipelineGraph.PerfTab.Name" ) );

    // Create a composite, slam everything on there like it was in the history tab.
    //
    perfComposite = new Composite( pipelineGraph.extraViewTabFolder, SWT.NONE );
    perfComposite.setBackground( GUIResource.getInstance().getColorBackground() );
    perfComposite.setLayout( new FormLayout() );

    hopUi.getProps().setLook( perfComposite );

    pipelineGraph.getDisplay().asyncExec( new Runnable() {

      public void run() {
        setupContent();
      }
    } );

    pipelinePerfTab.setControl( perfComposite );

    pipelineGraph.extraViewTabFolder.setSelection( pipelinePerfTab );

    pipelineGraph.extraViewTabFolder.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent arg0 ) {
        layoutPerfComposite();
        updateGraph();
      }
    } );
  }

  public void setupContent() {
    PropsUI props = PropsUI.getInstance();

    // there is a potential infinite loop below if this method
    // is called when the pipeline-graph is not running, so we check
    // early to make sure it won't happen (see PDI-5009)
    if ( !pipelineGraph.isRunning()
      || pipelineGraph.pipeline == null || !pipelineGraph.pipeline.getSubject().isCapturingTransformPerformanceSnapShots() ) {
      showEmptyGraph();
      return; // TODO: display help text and rerty button
    }

    if ( perfComposite.isDisposed() ) {
      return;
    }

    // Remove anything on the perf composite, like an empty page message
    //
    for ( Control control : perfComposite.getChildren() ) {
      if ( !control.isDisposed() ) {
        control.dispose();
      }
    }

    emptyGraph = false;

    this.title = pipelineGraph.pipeline.getSubject().getName();
    this.timeDifference = pipelineGraph.pipeline.getSubject().getTransformPerformanceCapturingDelay();
    this.transformPerformanceSnapShots = pipelineGraph.pipeline.getEngineMetrics().getComponentPerformanceSnapshots();

    // Wait a second for the first data to pour in...
    // TODO: make this wait more elegant...
    //
    while ( this.transformPerformanceSnapShots == null || transformPerformanceSnapShots.isEmpty() ) {

      this.transformPerformanceSnapShots = pipelineGraph.pipeline.getEngineMetrics().getComponentPerformanceSnapshots();
      try {
        // Getting engine metrics is fairly expensive so wait long enough
        //
        Thread.sleep( 1000L );
      } catch ( InterruptedException e ) {
        // Ignore errors
      }
    }

    Set<IEngineComponent> transformsSet = transformPerformanceSnapShots.keySet();
    transforms = transformsSet.toArray( new IEngineComponent[ transformsSet.size() ] );
    Arrays.sort( transforms );

    String[] transformNames = new String[transforms.length];
    for (int i=0;i<transforms.length;i++) {
      transformNames[i] = transforms.toString();
    }

    // Display 2 lists with the data types and the transforms on the left side.
    // Then put a canvas with the graph on the right side
    //
    Label dataListLabel = new Label( perfComposite, SWT.NONE );
    dataListLabel.setText( BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Metrics.Label" ) );
    hopUi.getProps().setLook( dataListLabel );
    FormData fdDataListLabel = new FormData();

    fdDataListLabel.left = new FormAttachment( 0, 0 );
    fdDataListLabel.right = new FormAttachment( hopUi.getProps().getMiddlePct() / 2, props.getMargin() );
    fdDataListLabel.top = new FormAttachment( 0, props.getMargin() + 5 );
    dataListLabel.setLayoutData( fdDataListLabel );

    dataList = new org.eclipse.swt.widgets.List( perfComposite, SWT.MULTI
        | SWT.H_SCROLL | SWT.V_SCROLL | SWT.LEFT | SWT.BORDER );
    hopUi.getProps().setLook( dataList );
    dataList.setItems( dataChoices );
    dataList.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent event ) {

        // If there are multiple selections here AND there are multiple selections in the transforms list, we only take the
        // first transform in the selection...
        //
        if ( dataList.getSelectionCount() > 1 && transformsList.getSelectionCount() > 1 ) {
          transformsList.setSelection( transformsList.getSelectionIndices()[ 0 ] );
        }

        updateGraph();
      }
    } );

    FormData fdDataList = new FormData();
    fdDataList.left = new FormAttachment( 0, 0 );
    fdDataList.right = new FormAttachment( hopUi.getProps().getMiddlePct() / 2, props.getMargin() );
    fdDataList.top = new FormAttachment( dataListLabel, props.getMargin() );
    fdDataList.bottom = new FormAttachment( 40, props.getMargin() );
    dataList.setLayoutData( fdDataList );

    Label transformsListLabel = new Label( perfComposite, SWT.NONE );
    transformsListLabel.setText( BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Transforms.Label" ) );

    hopUi.getProps().setLook( transformsListLabel );

    FormData fdTransformsListLabel = new FormData();
    fdTransformsListLabel.left = new FormAttachment( 0, 0 );
    fdTransformsListLabel.right = new FormAttachment( hopUi.getProps().getMiddlePct() / 2, props.getMargin() );
    fdTransformsListLabel.top = new FormAttachment( dataList, props.getMargin() );
    transformsListLabel.setLayoutData( fdTransformsListLabel );

    transformsList = new org.eclipse.swt.widgets.List( perfComposite, SWT.MULTI
        | SWT.H_SCROLL | SWT.V_SCROLL | SWT.LEFT | SWT.BORDER );
    hopUi.getProps().setLook( transformsList );
    transformsList.setItems( transformNames );
    transformsList.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent event ) {

        // If there are multiple selections here AND there are multiple selections in the data list, we only take the
        // first data item in the selection...
        //
        if ( dataList.getSelectionCount() > 1 && transformsList.getSelectionCount() > 1 ) {
          dataList.setSelection( dataList.getSelectionIndices()[ 0 ] );
        }

        updateGraph();
      }
    } );
    FormData fdTransformsList = new FormData();
    fdTransformsList.left = new FormAttachment( 0, 0 );
    fdTransformsList.right = new FormAttachment( hopUi.getProps().getMiddlePct() / 2, props.getMargin() );
    fdTransformsList.top = new FormAttachment( transformsListLabel, props.getMargin() );
    fdTransformsList.bottom = new FormAttachment( 100, props.getMargin() );
    transformsList.setLayoutData( fdTransformsList );

    canvas = new Canvas( perfComposite, SWT.NONE );
    hopUi.getProps().setLook( canvas );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( hopUi.getProps().getMiddlePct() / 2, props.getMargin() );
    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.top = new FormAttachment( 0, props.getMargin() );
    fdCanvas.bottom = new FormAttachment( 100, 0 );
    canvas.setLayoutData( fdCanvas );

    perfComposite.addControlListener( new ControlAdapter() {
      public void controlResized( ControlEvent event ) {
        updateGraph();
      }
    } );

    perfComposite.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent event ) {
        if ( image != null ) {
          image.dispose();
        }
      }
    } );

    canvas.addPaintListener( new PaintListener() {

      public void paintControl( PaintEvent event ) {
        if ( image != null && !image.isDisposed() ) {
          event.gc.drawImage( image, 0, 0 );
        }
      }
    } );

    // Refresh automatically every 5 seconds as well.
    //
    final Timer timer = new Timer( "HopGuiPipelinePerfDelegate Timer" );
    timer.schedule( new TimerTask() {
      public void run() {
        updateGraph();
      }
    }, 0, 5000 );

    // When the tab is closed, we remove the update timer
    //
    pipelinePerfTab.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent arg0 ) {
        timer.cancel();
      }
    } );
  }

  /**
   * Tell the user that the pipeline is not running or that there is no monitoring configured.
   */
  private void showEmptyGraph() {
    if ( perfComposite.isDisposed() ) {
      return;
    }

    emptyGraph = true;

    Label label = new Label( perfComposite, SWT.CENTER );
    label.setText( BaseMessages.getString( PKG, "PipelineLog.Dialog.PerformanceMonitoringNotEnabled.Message" ) );
    label.setBackground( perfComposite.getBackground() );
    label.setFont( GUIResource.getInstance().getFontMedium() );

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment( 5, 0 );
    fdLabel.right = new FormAttachment( 95, 0 );
    fdLabel.top = new FormAttachment( 5, 0 );
    label.setLayoutData( fdLabel );

    Button button = new Button( perfComposite, SWT.CENTER );
    button.setText( BaseMessages.getString( PKG, "PipelineLog.Dialog.PerformanceMonitoring.Button" ) );
    button.setBackground( perfComposite.getBackground() );
    button.setFont( GUIResource.getInstance().getFontMedium() );

    button.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent event ) {
        pipelineGraph.editProperties( pipelineGraph.getPipelineMeta(), hopUi, true, PipelineDialog.Tabs.MONITOR_TAB );
      }
    } );

    FormData fdButton = new FormData();
    fdButton.left = new FormAttachment( 40, 0 );
    fdButton.right = new FormAttachment( 60, 0 );
    fdButton.top = new FormAttachment( label, 5 );
    button.setLayoutData( fdButton );

    perfComposite.layout( true, true );
  }

  public void showPerfView() {

    if ( pipelinePerfTab == null || pipelinePerfTab.isDisposed() ) {
      addPipelinePerf();
    } else {
      pipelinePerfTab.dispose();

      pipelineGraph.checkEmptyExtraView();
    }
  }

  private void updateGraph() {

    pipelineGraph.getDisplay().asyncExec( new Runnable() {

      public void run() {
        if ( perfComposite != null
          && !perfComposite.isDisposed() && canvas != null && !canvas.isDisposed() && pipelinePerfTab != null
          && !pipelinePerfTab.isDisposed() ) {
          if ( pipelinePerfTab.isShowing() ) {
            updateCanvas();
          }
        }
      }

    } );
  }

  private void updateCanvas() {
    Rectangle bounds = canvas.getBounds();
    if ( bounds.width <= 0 || bounds.height <= 0 ) {
      return;
    }

    // The list of snapshots : convert to JFreeChart dataset
    //
    DefaultCategoryDataset dataset = new DefaultCategoryDataset();

    String[] selectedTransforms = transformsList.getSelection();
    if ( selectedTransforms == null || selectedTransforms.length == 0 ) {
      selectedTransforms = new String[] { transforms[ 0 ].toString(), }; // first transform
      transformsList.select( 0 );
    }
    int[] dataIndices = dataList.getSelectionIndices();
    if ( dataIndices == null || dataIndices.length == 0 ) {
      dataIndices = new int[] { DATA_CHOICE_WRITTEN, };
      dataList.select( 0 );
    }

    boolean multiTransform = transformsList.getSelectionCount() > 1;
    boolean multiData = dataList.getSelectionCount() > 1;
    boolean calcMoving = !multiTransform && !multiData; // A single metric shown for a single transform
    List<Double> movingList = new ArrayList<Double>();
    int movingSize = 10;
    double movingTotal = 0;
    int totalTimeInSeconds = 0;

    for ( int t = 0; t < selectedTransforms.length; t++ ) {

      String transformNameCopy = selectedTransforms[ t ];

      List<PerformanceSnapShot> snapShotList = transformPerformanceSnapShots.get( transformNameCopy );
      if ( snapShotList != null && snapShotList.size() > 1 ) {
        totalTimeInSeconds =
          (int) Math
            .round( ( (double) ( snapShotList.get( snapShotList.size() - 1 ).getDate().getTime() - snapShotList
              .get( 0 ).getDate().getTime() ) ) / 1000 );
        for ( int i = 0; i < snapShotList.size(); i++ ) {
          PerformanceSnapShot snapShot = snapShotList.get( i );
          if ( snapShot.getTimeDifference() != 0 ) {

            double factor = (double) 1000 / (double) snapShot.getTimeDifference();

            for ( int d = 0; d < dataIndices.length; d++ ) {

              String dataType;
              if ( multiTransform ) {
                dataType = transformNameCopy;
              } else {
                dataType = dataChoices[ dataIndices[ d ] ];
              }
              String xLabel = Integer.toString( Math.round( i * timeDifference / 1000 ) );
              Double metric = null;
              switch ( dataIndices[ d ] ) {
                case DATA_CHOICE_INPUT:
                  metric = snapShot.getLinesInput() * factor;
                  break;
                case DATA_CHOICE_OUTPUT:
                  metric = snapShot.getLinesOutput() * factor;
                  break;
                case DATA_CHOICE_READ:
                  metric = snapShot.getLinesRead() * factor;
                  break;
                case DATA_CHOICE_WRITTEN:
                  metric = snapShot.getLinesWritten() * factor;
                  break;
                case DATA_CHOICE_UPDATED:
                  metric = snapShot.getLinesUpdated() * factor;
                  break;
                case DATA_CHOICE_REJECTED:
                  metric = snapShot.getLinesRejected() * factor;
                  break;
                case DATA_CHOICE_INPUT_BUFFER_SIZE:
                  metric = (double) snapShot.getInputBufferSize();
                  break;
                case DATA_CHOICE_OUTPUT_BUFFER_SIZE:
                  metric = (double) snapShot.getOutputBufferSize();
                  break;
                default:
                  break;
              }
              if ( metric != null ) {
                dataset.addValue( metric, dataType, xLabel );

                if ( calcMoving ) {
                  movingTotal += metric;
                  movingList.add( metric );
                  if ( movingList.size() > movingSize ) {
                    movingTotal -= movingList.get( 0 );
                    movingList.remove( 0 );
                  }
                  double movingAverage = movingTotal / movingList.size();
                  dataset.addValue( movingAverage, dataType + "(Avg)", xLabel );
                }
              }
            }
          }
        }
      }
    }
    String chartTitle = title;
    if ( multiTransform ) {
      chartTitle += " (" + dataChoices[ dataIndices[ 0 ] ] + ")";
    } else {
      chartTitle += " (" + selectedTransforms[ 0 ] + ")";
    }
    final JFreeChart chart =
      ChartFactory.createLineChart( chartTitle, // chart title
        BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.TimeInSeconds.Label", Integer
          .toString( totalTimeInSeconds ), Long.toString( timeDifference ) ), // domain axis label
        BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.RowsPerSecond.Label" ), // range axis label
        dataset, // data
        PlotOrientation.VERTICAL, // orientation
        true, // include legend
        true, // tooltips
        false ); // urls       
    chart.setBackgroundPaint( Color.white );
    TextTitle title = new TextTitle( chartTitle );
    // title.setExpandToFitSpace(true);
    // org.eclipse.swt.graphics.Color pentahoColor = GUIResource.getInstance().getColorPentaho();
    // java.awt.Color color = new java.awt.Color(pentahoColor.getRed(), pentahoColor.getGreen(),pentahoColor.getBlue());
    // title.setBackgroundPaint(color);
    title.setFont( new Font( "SansSerif", Font.BOLD, 12 ) );
    chart.setTitle( title );
    CategoryPlot plot = (CategoryPlot) chart.getPlot();
    plot.setBackgroundPaint( Color.white );
    plot.setForegroundAlpha( 0.5f );
    plot.setRangeGridlinesVisible( true );

    NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
    rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

    CategoryAxis domainAxis = plot.getDomainAxis();
    domainAxis.setTickLabelsVisible( false );

    // Customize the renderer...
    //
    LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot.getRenderer();
    renderer.setBaseShapesVisible( true );
    renderer.setDrawOutlines( true );
    renderer.setUseFillPaint( true );
    renderer.setBaseFillPaint( Color.white );
    renderer.setSeriesStroke( 0, new BasicStroke( 1.5f ) );
    renderer.setSeriesOutlineStroke( 0, new BasicStroke( 1.5f ) );
    renderer.setSeriesStroke( 1, new BasicStroke( 2.5f ) );
    renderer.setSeriesOutlineStroke( 1, new BasicStroke( 2.5f ) );
    renderer.setSeriesShape( 0, new Ellipse2D.Double( -3.0, -3.0, 6.0, 6.0 ) );

    BufferedImage bufferedImage = chart.createBufferedImage( bounds.width, bounds.height );
    ImageData imageData = ImageUtil.convertToSWT( bufferedImage );

    // dispose previous image...
    //
    if ( image != null ) {
      image.dispose();
    }
    image = new Image( pipelineGraph.getDisplay(), imageData );

    // Draw the image on the canvas...
    //
    canvas.redraw();
  }

  /**
   * @return the pipelineHistoryTab
   */
  public CTabItem getPipelinePerfTab() {
    return pipelinePerfTab;
  }

  /**
   * @return the emptyGraph
   */
  public boolean isEmptyGraph() {
    return emptyGraph;
  }

  public void layoutPerfComposite() {
    if ( !perfComposite.isDisposed() ) {
      perfComposite.layout( true, true );
    }
  }

}
