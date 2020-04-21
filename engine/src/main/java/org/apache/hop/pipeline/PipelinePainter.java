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

package org.apache.hop.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.IPrimitiveGc.EColor;
import org.apache.hop.core.gui.IPrimitiveGc.EFont;
import org.apache.hop.core.gui.IPrimitiveGc.EImage;
import org.apache.hop.core.gui.IPrimitiveGc.ELineStyle;
import org.apache.hop.core.gui.IScrollBar;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;

import java.util.List;
import java.util.Map;

public class PipelinePainter extends BasePainter<PipelineHopMeta, TransformMeta> {

  private static Class<?> PKG = PipelinePainter.class; // for i18n purposes, needed by Translator!!

  public static final String STRING_PARTITIONING_CURRENT_TRANSFORM = "PartitioningCurrentTransform";
  public static final String STRING_REMOTE_INPUT_TRANSFORMS = "RemoteInputTransforms";
  public static final String STRING_REMOTE_OUTPUT_TRANSFORMS = "RemoteOutputTransforms";
  public static final String STRING_TRANSFORM_ERROR_LOG = "TransformErrorLog";
  public static final String STRING_HOP_TYPE_COPY = "HopTypeCopy";
  public static final String STRING_ROW_DISTRIBUTION = "RowDistribution";

  private PipelineMeta pipelineMeta;

  private Map<String, String> transformLogMap;
  private TransformMeta startHopTransform;
  private Point endHopLocation;
  private TransformMeta endHopTransform;
  private TransformMeta noInputTransform;
  private StreamType candidateHopType;
  private boolean startErrorHopTransform;
  private TransformMeta showTargetStreamsTransform;
  private IPipelineEngine<PipelineMeta> pipeline;
  private boolean slowTransformIndicatorEnabled;

  private EngineMetrics engineMetrics;

  public static final String[] magnificationDescriptions =
    new String[] { "  400% ", "  200% ", "  150% ", "  100% ", "  75% ", "  50% ", "  25% " };


  public PipelinePainter( IGc gc, PipelineMeta pipelineMeta, Point area, IScrollBar hori,
                          IScrollBar vert, PipelineHopMeta candidate, Point drop_candidate, Rectangle selrect,
                          List<AreaOwner> areaOwners, int iconsize, int linewidth, int gridsize,
                          String noteFontName, int noteFontHeight, IPipelineEngine<PipelineMeta> pipeline,
                          boolean slowTransformIndicatorEnabled, double zoomFactor ) {
    super( gc, pipelineMeta, area, hori, vert, drop_candidate, selrect, areaOwners, iconsize, linewidth, gridsize,
      noteFontName, noteFontHeight, zoomFactor );
    this.pipelineMeta = pipelineMeta;

    this.candidate = candidate;

    this.pipeline = pipeline;
    this.slowTransformIndicatorEnabled = slowTransformIndicatorEnabled;

    transformLogMap = null;
  }

  public PipelinePainter( IGc gc, PipelineMeta pipelineMeta, Point area, IScrollBar hori,
                          IScrollBar vert, PipelineHopMeta candidate, Point drop_candidate, Rectangle selrect,
                          List<AreaOwner> areaOwners, int iconsize, int linewidth, int gridsize,
                          boolean antiAliasing, String noteFontName, int noteFontHeight, double zoomFactor ) {

    this( gc, pipelineMeta, area, hori, vert, candidate, drop_candidate, selrect, areaOwners, iconsize,
      linewidth, gridsize, noteFontName, noteFontHeight, null, false, zoomFactor );
  }

  private static String[] getPeekTitles() {
    String[] titles =
      {

        BaseMessages.getString( PKG, "PeekMetric.Column.Copynr" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Read" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Written" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Input" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Output" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Updated" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Rejected" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Errors" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Active" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Time" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.Speed" ),
        BaseMessages.getString( PKG, "PeekMetric.Column.PriorityBufferSizes" ) };
    return titles;
  }

  public void buildPipelineImage() {
    if ( pipeline == null ) {
      engineMetrics = new EngineMetrics();
    } else {
      engineMetrics = pipeline.getEngineMetrics();
    }

    Point max = pipelineMeta.getMaximum();
    Point thumb = getThumb( area, max );
    offset = getOffset( thumb, area );

    // First clear the image in the background color
    gc.setBackground( EColor.BACKGROUND );
    gc.fillRectangle( 0, 0, area.x, area.y );

    // Draw the pipeline onto the image
    //
    gc.setTransform( translationX, translationY, 0, magnification );
    gc.setAlpha( 255 );
    drawPipeline( thumb );

    gc.dispose();
  }

  private void drawPipeline( Point thumb ) {
    if ( gridSize > 1 ) {
      drawGrid();
    }

    if ( hori != null && vert != null ) {
      hori.setThumb( thumb.x );
      vert.setThumb( thumb.y );
    }

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelinePainterStart.id, this );
    } catch ( HopException e ) {
      LogChannel.GENERAL.logError( "Error in PipelinePainterStart extension point", e );
    }

    gc.setFont( EFont.NOTE );

    // First the notes
    for ( int i = 0; i < pipelineMeta.nrNotes(); i++ ) {
      NotePadMeta ni = pipelineMeta.getNote( i );
      drawNote( ni );
    }

    gc.setFont( EFont.GRAPH );
    gc.setBackground( EColor.BACKGROUND );

    for ( int i = 0; i < pipelineMeta.nrPipelineHops(); i++ ) {
      PipelineHopMeta hi = pipelineMeta.getPipelineHop( i );
      drawHop( hi );
    }

    EImage arrow;
    if ( candidate != null ) {
      drawHop( candidate, true );
    } else {
      if ( startHopTransform != null && endHopLocation != null ) {
        Point fr = startHopTransform.getLocation();
        Point to = endHopLocation;
        if ( endHopTransform == null ) {
          gc.setForeground( EColor.GRAY );
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground( EColor.BLUE );
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen( fr.x + iconSize / 2, fr.y + iconSize / 2 );
        Point end = real2screen( to.x, to.y );
        drawArrow( arrow, start.x, start.y, end.x, end.y, theta, calcArrowLength(), 1.2, null, startHopTransform,
          endHopTransform == null ? endHopLocation : endHopTransform );
      } else if ( endHopTransform != null && endHopLocation != null ) {
        Point fr = endHopLocation;
        Point to = endHopTransform.getLocation();
        if ( startHopTransform == null ) {
          gc.setForeground( EColor.GRAY );
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground( EColor.BLUE );
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen( fr.x, fr.y );
        Point end = real2screen( to.x + iconSize / 2, to.y + iconSize / 2 );
        drawArrow( arrow, start.x, start.y, end.x, end.y, theta, calcArrowLength(), 1.2, null, startHopTransform == null
          ? endHopLocation : startHopTransform, endHopTransform );
      }

    }

    // Draw regular transform appearance
    for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      drawTransform( transformMeta );
    }

    if ( slowTransformIndicatorEnabled ) {

      // Highlight possible bottlenecks
      for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
        TransformMeta transformMeta = pipelineMeta.getTransform( i );
        checkDrawSlowTransformIndicator( transformMeta );
      }

    }

    // Draw transform status indicators (running vs. done)
    for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      drawTransformStatusIndicator( transformMeta );
    }

    // Draw performance table for selected transform(s)
    for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      drawTransformPerformanceTable( transformMeta );
    }

    int selectedTransformsCount = 0;
    for ( int i = pipelineMeta.nrTransforms() - 1; i >= 0; i-- ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      if ( transformMeta.isSelected() ) {
        selectedTransformsCount++;
      }
    }

    PipelinePainterFlyoutExtension extension = null;
    for ( int i = pipelineMeta.nrTransforms() - 1; i >= 0; i-- ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      if ( transformMeta.isSelected() && selectedTransformsCount == 1 ) {
        extension = new PipelinePainterFlyoutExtension(
          gc, areaOwners, pipelineMeta, transformMeta, translationX, translationY, magnification, area, offset );
        break;
      }
    }
    if ( extension == null ) {
      // pass null to notify extension that nothing is selected
      extension = new PipelinePainterFlyoutExtension(
        gc, areaOwners, pipelineMeta, null, translationX, translationY, magnification, area, offset );
    }
    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelinePainterFlyout.id, extension );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error calling extension point(s) for the pipeline painter transform", e );
    }

    // Display an icon on the indicated location signaling to the user that the transform in question does not accept input
    //
    if ( noInputTransform != null ) {
      gc.setLineWidth( 2 );
      gc.setForeground( EColor.RED );
      Point n = noInputTransform.getLocation();
      gc.drawLine( n.x - 5, n.y - 5, n.x + iconSize + 10, n.y + iconSize + 10 );
      gc.drawLine( n.x - 5, n.y + iconSize + 5, n.x + iconSize + 5, n.y - 5 );
    }

    if ( dropCandidate != null ) {
      gc.setLineStyle( ELineStyle.SOLID );
      gc.setForeground( EColor.BLACK );
      Point screen = real2screen( dropCandidate.x, dropCandidate.y );
      gc.drawRectangle( screen.x, screen.y, iconSize, iconSize );
    }

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelinePainterEnd.id, this );
    } catch ( HopException e ) {
      LogChannel.GENERAL.logError( "Error in PipelinePainterEnd extension point", e );
    }

    drawRect( selectionRectangle );
  }

  private void checkDrawSlowTransformIndicator( TransformMeta transformMeta ) {

    if ( transformMeta == null ) {
      return;
    }

    // draw optional performance indicator
    if ( pipeline != null ) {

      Point pt = transformMeta.getLocation();
      if ( pt == null ) {
        pt = new Point( 50, 50 );
      }

      Point screen = real2screen( pt.x, pt.y );
      int x = screen.x;
      int y = screen.y;

      synchronized ( engineMetrics ) {
        List<IEngineComponent> components = engineMetrics.getComponents();
        for ( IEngineComponent component : components ) {
          if ( component.getName().equals( transformMeta.getName() ) ) {
            if ( component.isRunning() ) {
              Long inputRowsValue = engineMetrics.getComponentMetric( component, Pipeline.METRIC_BUFFER_IN );
              Long outputRowsValue = engineMetrics.getComponentMetric( component, Pipeline.METRIC_BUFFER_OUT );
              if ( inputRowsValue != null && outputRowsValue != null ) {
                long inputRows = inputRowsValue.longValue();
                long outputRows = outputRowsValue.longValue();

                // if the transform can't keep up with its input, mark it by drawing an animation
                boolean isSlow = inputRows * 0.85 > outputRows;
                if ( isSlow ) {
                  gc.setLineWidth( lineWidth + 1 );
                  if ( System.currentTimeMillis() % 2000 > 1000 ) {
                    gc.setForeground( EColor.BACKGROUND );
                    gc.setLineStyle( ELineStyle.SOLID );
                    gc.drawRectangle( x + 1, y + 1, iconSize - 2, iconSize - 2 );

                    gc.setForeground( EColor.DARKGRAY );
                    gc.setLineStyle( ELineStyle.DOT );
                    gc.drawRectangle( x + 1, y + 1, iconSize - 2, iconSize - 2 );
                  } else {
                    gc.setForeground( EColor.DARKGRAY );
                    gc.setLineStyle( ELineStyle.SOLID );
                    gc.drawRectangle( x + 1, y + 1, iconSize - 2, iconSize - 2 );

                    gc.setForeground( EColor.BACKGROUND );
                    gc.setLineStyle( ELineStyle.DOT );
                    gc.drawRectangle( x + 1, y + 1, iconSize - 2, iconSize - 2 );
                  }
                }
              }
            }
            gc.setLineStyle( ELineStyle.SOLID );
          }
        }
      }
    }
  }

  private void drawTransformPerformanceTable( TransformMeta transformMeta ) {

    if ( transformMeta == null ) {
      return;
    }

    // draw optional performance indicator
    if ( pipeline != null ) {

      Point pt = transformMeta.getLocation();
      if ( pt == null ) {
        pt = new Point( 50, 50 );
      }

      Point screen = real2screen( pt.x, pt.y );
      int x = screen.x;
      int y = screen.y;

      List<IEngineComponent> transforms = pipeline.getComponentCopies( transformMeta.getName() );

      // draw mouse over performance indicator
      if ( pipeline.isRunning() ) {

        if ( transformMeta.isSelected() ) {

          // determine popup dimensions up front
          int popupX = x;
          int popupY = y;

          int popupWidth = 0;
          int popupHeight = 1;

          gc.setFont( EFont.SMALL );
          Point p = gc.textExtent( "0000000000" );
          int colWidth = p.x + MINI_ICON_MARGIN;
          int rowHeight = p.y + MINI_ICON_MARGIN;
          int titleWidth = 0;

          // calculate max title width to get the colum with
          String[] titles = PipelinePainter.getPeekTitles();

          for ( String title : titles ) {
            Point titleExtent = gc.textExtent( title );
            titleWidth = Math.max( titleExtent.x + MINI_ICON_MARGIN, titleWidth );
            popupHeight += titleExtent.y + MINI_ICON_MARGIN;
          }

          popupWidth = titleWidth + 2 * MINI_ICON_MARGIN;

          // determine total popup width
          popupWidth += transforms.size() * colWidth;

          // determine popup position
          popupX = popupX + ( iconSize - popupWidth ) / 2;
          popupY = popupY - popupHeight - MINI_ICON_MARGIN;

          // draw the frame
          gc.setForeground( EColor.DARKGRAY );
          gc.setBackground( EColor.LIGHTGRAY );
          gc.setLineWidth( 1 );
          gc.fillRoundRectangle( popupX, popupY, popupWidth, popupHeight, 7, 7 );
          // draw the title columns
          // gc.setBackground(EColor.BACKGROUND);
          // gc.fillRoundRectangle(popupX, popupY, titleWidth+MINI_ICON_MARGIN, popupHeight, 7, 7);
          gc.setBackground( EColor.LIGHTGRAY );
          gc.drawRoundRectangle( popupX, popupY, popupWidth, popupHeight, 7, 7 );

          for ( int i = 0, barY = popupY; i < titles.length; i++ ) {
            // fill each line with a slightly different background color

            if ( i % 2 == 1 ) {
              gc.setBackground( EColor.BACKGROUND );
            } else {
              gc.setBackground( EColor.LIGHTGRAY );
            }
            gc.fillRoundRectangle( popupX + 1, barY + 1, popupWidth - 2, rowHeight, 7, 7 );
            barY += rowHeight;

          }

          // draw the header column
          int rowY = popupY + MINI_ICON_MARGIN;
          int rowX = popupX + MINI_ICON_MARGIN;

          gc.setForeground( EColor.BLACK );
          gc.setBackground( EColor.BACKGROUND );

          for ( int i = 0; i < titles.length; i++ ) {
            if ( i % 2 == 1 ) {
              gc.setBackground( EColor.BACKGROUND );
            } else {
              gc.setBackground( EColor.LIGHTGRAY );
            }
            gc.drawText( titles[ i ], rowX, rowY );
            rowY += rowHeight;
          }

          // draw the values for each copy of the transform
          gc.setBackground( EColor.LIGHTGRAY );
          rowX += titleWidth;

          for ( IEngineComponent transform : transforms ) {

            rowX += colWidth;
            rowY = popupY + MINI_ICON_MARGIN;

            String[] fields = getPeekFields( engineMetrics, transform );

            for ( int i = 0; i < fields.length; i++ ) {
              if ( i % 2 == 1 ) {
                gc.setBackground( EColor.BACKGROUND );
              } else {
                gc.setBackground( EColor.LIGHTGRAY );
              }
              drawTextRightAligned( fields[ i ], rowX, rowY );
              rowY += rowHeight;
            }

          }

        }
      }

    }
  }

  public String[] getPeekFields( EngineMetrics engineMetrics, IEngineComponent transform ) {
    String[] fields =
      new String[] {
        Integer.toString( transform.getCopyNr() ),
        Long.toString( engineMetrics.getComponentMetric( transform, Pipeline.METRIC_READ ) ),
        Long.toString( engineMetrics.getComponentMetric( transform, Pipeline.METRIC_WRITTEN ) ),
        Long.toString( engineMetrics.getComponentMetric( transform, Pipeline.METRIC_INPUT ) ),
        Long.toString( engineMetrics.getComponentMetric( transform, Pipeline.METRIC_OUTPUT ) ),
        Long.toString( engineMetrics.getComponentMetric( transform, Pipeline.METRIC_REJECTED ) ),
        Long.toString( engineMetrics.getComponentMetric( transform, Pipeline.METRIC_ERROR ) ),
        engineMetrics.getComponentStatusMap().get( transform ),
      };
    return fields;

  }

  private void drawTransformStatusIndicator( TransformMeta transformMeta ) {

    if ( transformMeta == null ) {
      return;
    }

    // draw status indicator
    if ( pipeline != null ) {

      Point pt = transformMeta.getLocation();
      if ( pt == null ) {
        pt = new Point( 50, 50 );
      }

      Point screen = real2screen( pt.x, pt.y );
      int x = screen.x;
      int y = screen.y;

      if ( pipeline != null ) {
        List<IEngineComponent> transforms = pipeline.getComponentCopies( transformMeta.getName() );

        for ( IEngineComponent transform : transforms ) {
          String transformStatus = engineMetrics.getComponentStatusMap().get( transform );
          if ( transformStatus != null && transformStatus.equalsIgnoreCase( EngineComponent.ComponentExecutionStatus.STATUS_FINISHED.getDescription() ) ) {
            gc.drawImage( EImage.TRUE, ( x + iconSize ) - ( MINI_ICON_SIZE / 2 ) + 4, y - ( MINI_ICON_SIZE / 2 ) - 1, magnification );
          }
        }
      }
    }
  }

  private void drawTextRightAligned( String txt, int x, int y ) {
    int off = gc.textExtent( txt ).x;
    x -= off;
    gc.drawText( txt, x, y );
  }

  private void drawHop( PipelineHopMeta hi ) {
    drawHop( hi, false );
  }

  private void drawHop( PipelineHopMeta hi, boolean isCandidate ) {
    TransformMeta fs = hi.getFromTransform();
    TransformMeta ts = hi.getToTransform();

    if ( fs != null && ts != null ) {
      drawLine( fs, ts, hi, isCandidate );
    }
  }

  private void drawTransform( TransformMeta transformMeta ) {
    if ( transformMeta == null ) {
      return;
    }
    boolean isDeprecated = transformMeta.isDeprecated();
    int alpha = gc.getAlpha();

    ITransformIOMeta ioMeta = transformMeta.getTransformMetaInterface().getTransformIOMeta();

    Point pt = transformMeta.getLocation();
    if ( pt == null ) {
      pt = new Point( 50, 50 );
    }

    Point screen = real2screen( pt.x, pt.y );
    int x = screen.x;
    int y = screen.y;

    boolean transformError = false;
    if ( transformLogMap != null && !transformLogMap.isEmpty() ) {
      String log = transformLogMap.get( transformMeta.getName() );
      if ( !Utils.isEmpty( log ) ) {
        transformError = true;
      }
    }

    // PARTITIONING

    // If this transform is partitioned, we're drawing a small symbol indicating this...
    //
    if ( transformMeta.isPartitioned() ) {
      gc.setLineWidth( 1 );
      gc.setForeground( EColor.RED );
      gc.setBackground( EColor.BACKGROUND );
      gc.setFont( EFont.GRAPH );

      PartitionSchema partitionSchema = transformMeta.getTransformPartitioningMeta().getPartitionSchema();
      if ( partitionSchema != null ) {
        String nrInput = "Px" + partitionSchema.calculatePartitionIds().size();

        Point textExtent = gc.textExtent( nrInput );
        textExtent.x += 2; // add a tiny little bit of a margin
        textExtent.y += 2;

        // Draw it a 2 icons above the transform icon.
        // Draw it an icon and a half to the left
        //
        Point point = new Point( x - iconSize - iconSize / 2, y - iconSize - iconSize );
        gc.drawRectangle( point.x, point.y, textExtent.x, textExtent.y );
        gc.drawText( nrInput, point.x + 1, point.y + 1 );

        // Now we draw an arrow from the cube to the transform...
        //
        gc.drawLine( point.x + textExtent.x, point.y + textExtent.y / 2, x - iconSize / 2, point.y
          + textExtent.y / 2 );
        gc.drawLine( x - iconSize / 2, point.y + textExtent.y / 2, x + iconSize / 3, y );

        // Also draw the name of the partition schema below the box
        //
        gc.setForeground( EColor.GRAY );
        gc.drawText( Const.NVL( partitionSchema.getName(), "<no partition name>" ), point.x, point.y
          + textExtent.y + 3, true );

        // Add to the list of areas...
        //
        areaOwners.add( new AreaOwner(
          AreaType.TRANSFORM_PARTITIONING, point.x, point.y, textExtent.x, textExtent.y, offset, transformMeta,
          STRING_PARTITIONING_CURRENT_TRANSFORM ) );

      }
    }

    String name = transformMeta.getName();

    if ( transformMeta.isSelected() ) {
      gc.setLineWidth( lineWidth + 2 );
    } else {
      gc.setLineWidth( lineWidth );
    }

    // Add to the list of areas...
    areaOwners.add( new AreaOwner( AreaType.TRANSFORM_ICON, x, y, iconSize, iconSize, offset, pipelineMeta, transformMeta ) );

    gc.setBackground( EColor.BACKGROUND );
    gc.fillRoundRectangle( x - 1, y - 1, iconSize + 1, iconSize + 1, 8, 8 );
    gc.drawTransformIcon( x, y, transformMeta, magnification );
    if ( transformError || transformMeta.isMissing() ) {
      gc.setForeground( EColor.RED );
    } else if ( isDeprecated ) {
      gc.setForeground( EColor.DEPRECATED );
    } else {
      gc.setForeground( EColor.CRYSTAL );
    }
    if ( transformMeta.isSelected() ) {
      if ( isDeprecated ) {
        gc.setForeground( EColor.DEPRECATED );
      } else {
        gc.setForeground( 0, 93, 166 );
      }
    }
    gc.drawRoundRectangle( x - 1, y - 1, iconSize + 1, iconSize + 1, 8, 8 );

    Point namePosition = getNamePosition( name, screen, iconSize );

    if ( transformMeta.isSelected() ) {
      int tmpAlpha = gc.getAlpha();
      gc.setAlpha( 192 );
      gc.setBackground( 216, 230, 241 );
      gc.fillRoundRectangle( namePosition.x - 8, namePosition.y - 2, gc.textExtent( name ).x + 15, 25,
        BasePainter.CORNER_RADIUS_5 + 15, BasePainter.CORNER_RADIUS_5 + 15 );
      gc.setAlpha( tmpAlpha );
    }

    gc.setForeground( EColor.BLACK );
    gc.setFont( EFont.GRAPH );
    gc.drawText( name, namePosition.x, namePosition.y + 2, true );
    boolean partitioned = false;

    TransformPartitioningMeta meta = transformMeta.getTransformPartitioningMeta();
    if ( transformMeta.isPartitioned() && meta != null ) {
      partitioned = true;
    }


    if ( !transformMeta.getCopiesString().equals( "1" ) && !partitioned ) {
      gc.setBackground( EColor.BACKGROUND );
      gc.setForeground( EColor.BLACK );
      String copies = "x" + transformMeta.getCopiesString();
      Point textExtent = gc.textExtent( copies );

      gc.drawText( copies, x - textExtent.x + 1, y - textExtent.y + 1, false );
      areaOwners.add( new AreaOwner( AreaType.TRANSFORM_COPIES_TEXT, x - textExtent.x + 1, y - textExtent.y + 1, textExtent.x, textExtent.y, offset, pipelineMeta, transformMeta ) );
    }

    // If there was an error during the run, the map "transformLogMap" is not empty and not null.
    //
    if ( transformError ) {
      String log = transformLogMap.get( transformMeta.getName() );

      // Show an error lines icon in the upper right corner of the transform...
      //
      int xError = ( x + iconSize ) - ( MINI_ICON_SIZE / 2 ) + 4;
      int yError = y - ( MINI_ICON_SIZE / 2 ) - 1;
      Point ib = gc.getImageBounds( EImage.TRANSFORM_ERROR_RED );
      gc.drawImage( EImage.TRANSFORM_ERROR_RED, xError, yError, magnification );

      areaOwners.add( new AreaOwner(
        AreaType.TRANSFORM_ERROR_RED_ICON, pt.x + iconSize - 3, pt.y - 8, ib.x, ib.y, offset, log,
        STRING_TRANSFORM_ERROR_LOG ) );
    }


    PipelinePainterExtension extension = new PipelinePainterExtension( gc, areaOwners, pipelineMeta, transformMeta, null, x, y, 0, 0, 0, 0, offset, iconSize );
    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelinePainterTransform.id, extension );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error calling extension point(s) for the pipeline painter transform", e );
    }

    // Restore the previous alpha value
    //
    gc.setAlpha( alpha );
  }

  public Point getNamePosition( String string, Point screen, int iconsize ) {
    Point textsize = gc.textExtent( string );

    int xpos = screen.x + ( iconsize / 2 ) - ( textsize.x / 2 );
    int ypos = screen.y + iconsize + 5;

    return new Point( xpos, ypos );
  }

  private void drawLine( TransformMeta fs, TransformMeta ts, PipelineHopMeta hi, boolean is_candidate ) {
    int[] line = getLine( fs, ts );

    EColor col;
    ELineStyle linestyle = ELineStyle.SOLID;
    int activeLinewidth = lineWidth;

    EImage arrow;
    if ( is_candidate ) {
      col = EColor.BLUE;
      arrow = EImage.ARROW_CANDIDATE;
    } else {
      if ( hi.isEnabled() ) {
        if ( fs.isSendingErrorRowsToTransform( ts ) ) {
          col = EColor.RED;
          linestyle = ELineStyle.DASH;
          activeLinewidth = lineWidth + 1;
          arrow = EImage.ARROW_ERROR;
        } else {
          col = EColor.HOP_DEFAULT;
          arrow = EImage.ARROW_DEFAULT;
        }
      } else {
        col = EColor.GRAY;
        arrow = EImage.ARROW_DISABLED;
      }
    }
    if ( hi.split ) {
      activeLinewidth = lineWidth + 2;
    }

    // Check to see if the source transform is an info transform for the target transform.
    //
    ITransformIOMeta ioMeta = ts.getTransformMetaInterface().getTransformIOMeta();
    List<IStream> infoStreams = ioMeta.getInfoStreams();
    if ( !infoStreams.isEmpty() ) {
      // Check this situation, the source transform can't run in multiple copies!
      //
      for ( IStream stream : infoStreams ) {
        if ( fs.getName().equalsIgnoreCase( stream.getTransformName() ) ) {
          // This is the info transform over this hop!
          //
          if ( fs.getCopies() > 1 ) {
            // This is not a desirable situation, it will always end in error.
            // As such, it's better not to give feedback on it.
            // We do this by drawing an error icon over the hop...
            //
            col = EColor.RED;
            arrow = EImage.ARROW_ERROR;
          }
        }
      }
    }

    gc.setForeground( col );
    gc.setLineStyle( linestyle );
    gc.setLineWidth( activeLinewidth );

    drawArrow( arrow, line, hi, fs, ts );

    if ( hi.split ) {
      gc.setLineWidth( lineWidth );
    }

    gc.setForeground( EColor.BLACK );
    gc.setBackground( EColor.BACKGROUND );
    gc.setLineStyle( ELineStyle.SOLID );
  }

  @Override
  protected void drawArrow( EImage arrow, int x1, int y1, int x2, int y2, double theta, int size, double factor,
                            PipelineHopMeta pipelineHop, Object startObject, Object endObject ) {
    int mx, my;
    int a, b, dist;
    double angle;

    gc.drawLine( x1, y1, x2, y2 );

    // in between 2 points
    mx = x1 + ( x2 - x1 ) / 2;
    my = y1 + ( y2 - y1 ) / 2;

    a = Math.abs( x2 - x1 );
    b = Math.abs( y2 - y1 );
    dist = (int) Math.sqrt( a * a + b * b );

    // determine factor (position of arrow to left side or right side
    // 0-->100%)
    if ( factor < 0 ) {
      if ( dist >= 2 * iconSize ) {
        factor = 1.3;
      } else {
        factor = 1.2;
      }
    }

    // in between 2 points
    mx = (int) ( x1 + factor * ( x2 - x1 ) / 2 );
    my = (int) ( y1 + factor * ( y2 - y1 ) / 2 );

    // calculate points for arrowhead
    // calculate points for arrowhead
    angle = Math.atan2( y2 - y1, x2 - x1 ) + ( Math.PI / 2 );

    boolean q1 = Math.toDegrees( angle ) >= 0 && Math.toDegrees( angle ) <= 90;
    boolean q2 = Math.toDegrees( angle ) > 90 && Math.toDegrees( angle ) <= 180;
    boolean q3 = Math.toDegrees( angle ) > 180 && Math.toDegrees( angle ) <= 270;
    boolean q4 = Math.toDegrees( angle ) > 270 || Math.toDegrees( angle ) < 0;

    if ( q1 || q3 ) {
      gc.drawImage( arrow, mx + 1, my, magnification, angle );
    } else if ( q2 || q4 ) {
      gc.drawImage( arrow, mx, my, magnification, angle );
    }

    if ( startObject instanceof TransformMeta && endObject instanceof TransformMeta ) {
      factor = 0.8;

      TransformMeta fs = (TransformMeta) startObject;
      TransformMeta ts = (TransformMeta) endObject;

      // in between 2 points
      mx = (int) ( x1 + factor * ( x2 - x1 ) / 2 ) - 8;
      my = (int) ( y1 + factor * ( y2 - y1 ) / 2 ) - 8;

      boolean errorHop = fs.isSendingErrorRowsToTransform( ts ) || ( startErrorHopTransform && fs.equals( startHopTransform ) );
      boolean targetHop =
        Const.indexOfString( ts.getName(), fs.getTransformMetaInterface().getTransformIOMeta().getTargetTransformNames() ) >= 0;

      if ( targetHop ) {
        ITransformIOMeta ioMeta = fs.getTransformMetaInterface().getTransformIOMeta();
        IStream targetStream = ioMeta.findTargetStream( ts );
        if ( targetStream != null ) {
          EImage hopsIcon = BasePainter.getStreamIconImage( targetStream.getStreamIcon() );
          Point bounds = gc.getImageBounds( hopsIcon );
          gc.drawImage( hopsIcon, mx, my, magnification );

          areaOwners.add( new AreaOwner(
            AreaType.TRANSFORM_TARGET_HOP_ICON, mx, my, bounds.x, bounds.y, offset, fs, targetStream ) );
        }
      } else if ( fs.isDistributes()
        && fs.getRowDistribution() != null && !ts.getTransformPartitioningMeta().isMethodMirror() && !errorHop ) {

        // Draw the custom row distribution plugin icon
        //
        EImage eImage = fs.getRowDistribution().getDistributionImage();
        if ( eImage != null ) {
          Point bounds = gc.getImageBounds( eImage );
          gc.drawImage( eImage, mx, my, magnification );

          areaOwners.add( new AreaOwner(
            AreaType.ROW_DISTRIBUTION_ICON, mx, my, bounds.x, bounds.y, offset, fs, STRING_ROW_DISTRIBUTION ) );
          mx += 16;
        }

      } else if ( !fs.isDistributes() && !ts.getTransformPartitioningMeta().isMethodMirror() && !errorHop ) {

        // Draw the copy icon on the hop
        //
        Point bounds = gc.getImageBounds( EImage.COPY_ROWS );
        gc.drawImage( EImage.COPY_ROWS, mx, my, magnification );

        areaOwners.add( new AreaOwner( AreaType.HOP_COPY_ICON, mx, my, bounds.x, bounds.y, offset, fs, STRING_HOP_TYPE_COPY ) );
        mx += 16;
      }

      if ( errorHop ) {
        Point bounds = gc.getImageBounds( EImage.COPY_ROWS );
        gc.drawImage( EImage.FALSE, mx, my, magnification );
        areaOwners.add( new AreaOwner( AreaType.HOP_ERROR_ICON, mx, my, bounds.x, bounds.y, offset, fs, ts ) );
        mx += 16;
      }

      ITransformIOMeta ioMeta = ts.getTransformMetaInterface().getTransformIOMeta();
      String[] infoTransformNames = ioMeta.getInfoTransformNames();

      if ( ( candidateHopType == StreamType.INFO && ts.equals( endHopTransform ) && fs.equals( startHopTransform ) )
        || Const.indexOfString( fs.getName(), infoTransformNames ) >= 0 ) {
        Point bounds = gc.getImageBounds( EImage.INFO );
        gc.drawImage( EImage.INFO, mx, my, magnification );
        areaOwners.add( new AreaOwner( AreaType.HOP_INFO_ICON, mx, my, bounds.x, bounds.y, offset, fs, ts ) );
        mx += 16;
      }

      // Check to see if the source transform is an info transform for the target transform.
      //
      if ( !Utils.isEmpty( infoTransformNames ) ) {
        // Check this situation, the source transform can't run in multiple copies!
        //
        for ( String infoTransform : infoTransformNames ) {
          if ( fs.getName().equalsIgnoreCase( infoTransform ) ) {
            // This is the info transform over this hop!
            //
            if ( fs.getCopies() > 1 ) {
              // This is not a desirable situation, it will always end in error.
              // As such, it's better not to give feedback on it.
              // We do this by drawing an error icon over the hop...
              //
              gc.drawImage( EImage.ERROR, mx, my, magnification );
              areaOwners.add( new AreaOwner( AreaType.HOP_INFO_TRANSFORM_COPIES_ERROR, mx, my, MINI_ICON_SIZE, MINI_ICON_SIZE, offset, fs, ts ) );
              mx += 16;

            }
          }
        }
      }

    }

    PipelinePainterExtension extension = new PipelinePainterExtension( gc, areaOwners, pipelineMeta, null, pipelineHop, x1, y1, x2, y2, mx, my, offset, iconSize );
    try {
      ExtensionPointHandler.callExtensionPoint(
        LogChannel.GENERAL, HopExtensionPoint.PipelinePainterArrow.id, extension );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error calling extension point(s) for the pipeline painter arrow", e );
    }
  }

  /**
   * @return the transformLogMap
   */
  public Map<String, String> getTransformLogMap() {
    return transformLogMap;
  }

  /**
   * @param transformLogMap the transformLogMap to set
   */
  public void setTransformLogMap( Map<String, String> transformLogMap ) {
    this.transformLogMap = transformLogMap;
  }

  /**
   * @param startHopTransform the start Hop Transform to set
   */
  public void setStartHopTransform( TransformMeta startHopTransform ) {
    this.startHopTransform = startHopTransform;
  }

  /**
   * @param endHopLocation the endHopLocation to set
   */
  public void setEndHopLocation( Point endHopLocation ) {
    this.endHopLocation = endHopLocation;
  }

  /**
   * @param noInputTransform the no Input Transform to set
   */
  public void setNoInputTransform( TransformMeta noInputTransform ) {
    this.noInputTransform = noInputTransform;
  }

  /**
   * @param endHopTransform the end Hop Transform to set
   */
  public void setEndHopTransform( TransformMeta endHopTransform ) {
    this.endHopTransform = endHopTransform;
  }

  public void setCandidateHopType( StreamType candidateHopType ) {
    this.candidateHopType = candidateHopType;
  }

  public void setStartErrorHopTransform( boolean startErrorHopTransform ) {
    this.startErrorHopTransform = startErrorHopTransform;
  }

  /**
   * @return the show Target Streams Transform
   */
  public TransformMeta getShowTargetStreamsTransform() {
    return showTargetStreamsTransform;
  }

  /**
   * @param showTargetStreamsTransform the show Target Streams Transform to set
   */
  public void setShowTargetStreamsTransform( TransformMeta showTargetStreamsTransform ) {
    this.showTargetStreamsTransform = showTargetStreamsTransform;
  }

  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  public IPipelineEngine<PipelineMeta> getPipeline() {
    return pipeline;
  }

  public void setPipeline( IPipelineEngine<PipelineMeta> pipeline ) {
    this.pipeline = pipeline;
  }

  public boolean isSlowTransformIndicatorEnabled() {
    return slowTransformIndicatorEnabled;
  }

  public void setSlowTransformIndicatorEnabled( boolean slowTransformIndicatorEnabled ) {
    this.slowTransformIndicatorEnabled = slowTransformIndicatorEnabled;
  }

  public TransformMeta getStartHopTransform() {
    return startHopTransform;
  }

  public Point getEndHopLocation() {
    return endHopLocation;
  }

  public TransformMeta getEndHopTransform() {
    return endHopTransform;
  }

  public TransformMeta getNoInputTransform() {
    return noInputTransform;
  }

  public StreamType getCandidateHopType() {
    return candidateHopType;
  }

  public boolean isStartErrorHopTransform() {
    return startErrorHopTransform;
  }
}
