/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.ui.hopgui.file.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.performance.PerformanceSnapShot;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.ImageUtil;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
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

public class TransformPerformanceSnapShotDialog extends Dialog {

  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

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

  private Shell parent, shell;
  private Map<String, List<PerformanceSnapShot>> transformPerformanceSnapShots;
  private Display display;
  private String[] transforms;
  private PropsUi props;
  private org.eclipse.swt.widgets.List transformsList;
  private Canvas canvas;
  private Image image;
  private long timeDifference;
  private String title;
  private org.eclipse.swt.widgets.List dataList;

  public TransformPerformanceSnapShotDialog( Shell parent, String title,
                                             Map<String, List<PerformanceSnapShot>> transformPerformanceSnapShots, long timeDifference ) {
    super( parent );
    this.parent = parent;
    this.display = parent.getDisplay();
    this.props = PropsUi.getInstance();
    this.timeDifference = timeDifference;
    this.title = title;
    this.transformPerformanceSnapShots = transformPerformanceSnapShots;

    Set<String> transformsSet = transformPerformanceSnapShots.keySet();
    transforms = transformsSet.toArray( new String[ transformsSet.size() ] );
    Arrays.sort( transforms );
  }

  public void open() {

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setText( BaseMessages.getString( PKG, "TransformPerformanceSnapShotDialog.Title" ) );
    shell.setImage( GuiResource.getInstance().getImageLogoSmall() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );

    // Display 2 lists with the data types and the transforms on the left side.
    // Then put a canvas with the graph on the right side
    //
    dataList =
      new org.eclipse.swt.widgets.List( shell, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.LEFT | SWT.BORDER );
    props.setLook( dataList );
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
    fdDataList.right = new FormAttachment( props.getMiddlePct() / 2, props.getMargin() );
    fdDataList.top = new FormAttachment( 0, 0 );
    fdDataList.bottom = new FormAttachment( 30, 0 );
    dataList.setLayoutData( fdDataList );

    transformsList =
      new org.eclipse.swt.widgets.List( shell, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.LEFT | SWT.BORDER );
    props.setLook( transformsList );
    transformsList.setItems( transforms );
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
    fdTransformsList.right = new FormAttachment( props.getMiddlePct() / 2, props.getMargin() );
    fdTransformsList.top = new FormAttachment( dataList, props.getMargin() );
    fdTransformsList.bottom = new FormAttachment( 100, props.getMargin() );
    transformsList.setLayoutData( fdTransformsList );

    canvas = new Canvas( shell, SWT.NONE );
    props.setLook( canvas );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( props.getMiddlePct() / 2, 0 );
    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.top = new FormAttachment( 0, 0 );
    fdCanvas.bottom = new FormAttachment( 100, 0 );
    canvas.setLayoutData( fdCanvas );

    shell.addControlListener( new ControlAdapter() {
      public void controlResized( ControlEvent event ) {
        updateGraph();
      }
    } );

    shell.addDisposeListener( event -> {
      if ( image != null ) {
        image.dispose();
      }
    } );

    canvas.addPaintListener( event -> {
      if ( image != null ) {
        event.gc.drawImage( image, 0, 0 );
      }
    } );

    // Refresh automatically every 5 seconds as well.
    //
    Timer timer = new Timer( "transform performance snapshot dialog Timer" );
    timer.schedule( new TimerTask() {
      public void run() {
        updateGraph();
      }
    }, 0, 5000 );

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
  }

  private void updateGraph() {

    display.asyncExec( () -> {
      if ( !shell.isDisposed() && !canvas.isDisposed() ) {
        updateCanvas();
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
      selectedTransforms = new String[] { transforms[ 0 ], }; // first transform
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
    image = new Image( display, imageData );

    // Draw the image on the canvas...
    //
    canvas.redraw();
  }

  /**
   * @return the shell
   */
  public Shell getShell() {
    return parent;
  }

  /**
   * @param shell the shell to set
   */
  public void setShell( Shell shell ) {
    this.parent = shell;
  }

  /**
   * @return the transformPerformanceSnapShots
   */
  public Map<String, List<PerformanceSnapShot>> getTransformPerformanceSnapShots() {
    return transformPerformanceSnapShots;
  }

  /**
   * @param transformPerformanceSnapShots the transformPerformanceSnapShots to set
   */
  public void setTransformPerformanceSnapShots( Map<String, List<PerformanceSnapShot>> transformPerformanceSnapShots ) {
    this.transformPerformanceSnapShots = transformPerformanceSnapShots;
  }

}
