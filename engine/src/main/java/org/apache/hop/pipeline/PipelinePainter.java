/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.IGc.EColor;
import org.apache.hop.core.gui.IGc.EFont;
import org.apache.hop.core.gui.IGc.EImage;
import org.apache.hop.core.gui.IGc.ELineStyle;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

public class PipelinePainter extends BasePainter<PipelineHopMeta, TransformMeta> {

  private static final Class<?> PKG = PipelinePainter.class; // For Translator

  public static final String STRING_PARTITIONING_CURRENT_TRANSFORM = "PartitioningCurrentTransform";
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
  private IPipelineEngine<PipelineMeta> pipeline;
  private boolean slowTransformIndicatorEnabled;
  private Map<String, RowBuffer> outputRowsMap;
  private Map<String, Object> stateMap;

  public static final String[] magnificationDescriptions =
      new String[] {"1000%", "800%", "600%", "400%", "200%", "150%", "100%", "75%", "50%", "25%"};

  public PipelinePainter(
      IGc gc,
      IVariables variables,
      PipelineMeta pipelineMeta,
      Point area,
      DPoint offset,
      PipelineHopMeta candidate,
      Rectangle selectRectangle,
      List<AreaOwner> areaOwners,
      int iconSize,
      int lineWidth,
      int gridSize,
      String noteFontName,
      int noteFontHeight,
      IPipelineEngine<PipelineMeta> pipeline,
      boolean slowTransformIndicatorEnabled,
      double zoomFactor,
      Map<String, RowBuffer> outputRowsMap,
      boolean drawingBorderAroundName,
      String mouseOverName,
      Map<String, Object> stateMap) {
    super(
        gc,
        variables,
        pipelineMeta,
        area,
        offset,
        selectRectangle,
        areaOwners,
        iconSize,
        lineWidth,
        gridSize,
        noteFontName,
        noteFontHeight,
        zoomFactor,
        drawingBorderAroundName,
        mouseOverName);
    this.pipelineMeta = pipelineMeta;

    this.candidate = candidate;

    this.pipeline = pipeline;
    this.slowTransformIndicatorEnabled = slowTransformIndicatorEnabled;

    this.outputRowsMap = outputRowsMap;

    transformLogMap = null;

    this.stateMap = stateMap;
  }

  public PipelinePainter(
      IGc gc,
      IVariables variables,
      PipelineMeta pipelineMeta,
      Point area,
      DPoint offset,
      PipelineHopMeta candidate,
      Rectangle selectionRectangle,
      List<AreaOwner> areaOwners,
      int iconSize,
      int lineWidth,
      int gridSize,
      String noteFontName,
      int noteFontHeight,
      double zoomFactor,
      boolean drawingEditIcons,
      String mouseOverName,
      Map<String, Object> stateMap) {
    this(
        gc,
        variables,
        pipelineMeta,
        area,
        offset,
        candidate,
        selectionRectangle,
        areaOwners,
        iconSize,
        lineWidth,
        gridSize,
        noteFontName,
        noteFontHeight,
        null,
        false,
        zoomFactor,
        new HashMap<>(),
        drawingEditIcons,
        mouseOverName,
        stateMap);
  }

  private static String[] getPeekTitles() {
    String[] titles = {
      BaseMessages.getString(PKG, "PeekMetric.Column.Copynr"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Read"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Written"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Input"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Output"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Updated"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Rejected"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Errors"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Active"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Time"),
      BaseMessages.getString(PKG, "PeekMetric.Column.Speed"),
      BaseMessages.getString(PKG, "PeekMetric.Column.PriorityBufferSizes")
    };
    return titles;
  }

  public void drawPipelineImage() throws HopException {
    // Make sure the canvas is scaled 100%
    gc.setTransform(0.0f, 0.0f, 1.0f);
    // First clear the image in the background color
    gc.setBackground(EColor.BACKGROUND);
    gc.fillRectangle(0, 0, area.x, area.y);

    // Draw the pipeline onto the image
    //
    gc.setTransform((float) offset.x, (float) offset.y, magnification);
    gc.setAlpha(255);
    drawPipeline();

    // Draw the navigation view in native pixels to make calculation a bit easier.
    //
    gc.setTransform(0.0f, 0.0f, 1.0f);
    drawNavigationView();

    gc.dispose();
  }

  private void drawPipeline() throws HopException {
    if (gridSize > 1) {
      drawGrid();
    }

    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelinePainterStart.id, this);
    } catch (HopException e) {
      LogChannel.GENERAL.logError("Error in PipelinePainterStart extension point", e);
    }

    gc.setFont(EFont.NOTE);

    // First the notes
    for (int i = 0; i < pipelineMeta.nrNotes(); i++) {
      NotePadMeta ni = pipelineMeta.getNote(i);
      drawNote(ni);
    }

    gc.setFont(EFont.GRAPH);
    gc.setBackground(EColor.BACKGROUND);

    for (int i = 0; i < pipelineMeta.nrPipelineHops(); i++) {
      PipelineHopMeta hi = pipelineMeta.getPipelineHop(i);
      drawHop(hi);
    }

    EImage arrow;
    if (candidate != null) {
      drawHop(candidate, true);
    } else {
      if (startHopTransform != null && endHopLocation != null) {
        Point fr = startHopTransform.getLocation();
        Point to = endHopLocation;
        if (endHopTransform == null) {
          gc.setForeground(EColor.GRAY);
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground(EColor.BLUE);
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen(fr.x + iconSize / 2, fr.y + iconSize / 2);
        Point end = real2screen(to.x, to.y);
        drawArrow(
            arrow,
            start.x,
            start.y,
            end.x,
            end.y,
            theta,
            calcArrowLength(),
            1.2,
            null,
            startHopTransform,
            endHopTransform == null ? endHopLocation : endHopTransform);
      } else if (endHopTransform != null && endHopLocation != null) {
        Point fr = endHopLocation;
        Point to = endHopTransform.getLocation();
        if (startHopTransform == null) {
          gc.setForeground(EColor.GRAY);
          arrow = EImage.ARROW_DISABLED;
        } else {
          gc.setForeground(EColor.BLUE);
          arrow = EImage.ARROW_DEFAULT;
        }
        Point start = real2screen(fr.x, fr.y);
        Point end = real2screen(to.x + iconSize / 2, to.y + iconSize / 2);
        drawArrow(
            arrow,
            start.x,
            start.y,
            end.x,
            end.y,
            theta,
            calcArrowLength(),
            1.2,
            null,
            startHopTransform == null ? endHopLocation : startHopTransform,
            endHopTransform);
      }
    }

    // Draw regular transform appearance
    for (int i = 0; i < pipelineMeta.nrTransforms(); i++) {
      TransformMeta transformMeta = pipelineMeta.getTransform(i);
      drawTransform(transformMeta);
    }

    if (slowTransformIndicatorEnabled) {

      // Highlight possible bottlenecks
      for (int i = 0; i < pipelineMeta.nrTransforms(); i++) {
        TransformMeta transformMeta = pipelineMeta.getTransform(i);
        checkDrawSlowTransformIndicator(transformMeta);
      }
    }

    // Draw transform status indicators (running vs. done)
    for (int i = 0; i < pipelineMeta.nrTransforms(); i++) {
      TransformMeta transformMeta = pipelineMeta.getTransform(i);
      drawTransformStatusIndicator(transformMeta);
    }

    // Draw data grid indicators (output data available)
    if (outputRowsMap != null && !outputRowsMap.isEmpty()) {
      for (int i = 0; i < pipelineMeta.nrTransforms(); i++) {
        TransformMeta transformMeta = pipelineMeta.getTransform(i);
        drawTransformOutputIndicator(transformMeta);
      }
    }

    // Draw performance table for selected transform(s)
    for (int i = 0; i < pipelineMeta.nrTransforms(); i++) {
      TransformMeta transformMeta = pipelineMeta.getTransform(i);
      drawTransformPerformanceTable(transformMeta);
    }

    // Display an icon on the indicated location signaling to the user that the transform in
    // question does not accept input
    //
    if (noInputTransform != null) {
      gc.setLineWidth(2);
      gc.setForeground(EColor.RED);
      Point n = noInputTransform.getLocation();
      gc.drawLine(n.x - 5, n.y - 5, n.x + iconSize + 10, n.y + iconSize + 10);
      gc.drawLine(n.x - 5, n.y + iconSize + 5, n.x + iconSize + 5, n.y - 5);
    }

    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelinePainterEnd.id, this);
    } catch (HopException e) {
      LogChannel.GENERAL.logError("Error in PipelinePainterEnd extension point", e);
    }

    drawRect(selectionRectangle);
  }

  private void checkDrawSlowTransformIndicator(TransformMeta transformMeta) {

    if (transformMeta == null) {
      return;
    }

    // draw optional performance indicator
    if (pipeline != null) {

      Point pt = transformMeta.getLocation();
      if (pt == null) {
        pt = new Point(50, 50);
      }

      Point screen = real2screen(pt.x, pt.y);
      int x = screen.x;
      int y = screen.y;

      List<IEngineComponent> components = pipeline.getComponents();
      for (IEngineComponent component : components) {
        if (component.getName().equals(transformMeta.getName())) {
          if (component.isRunning()) {
            Long inputRowsValue = component.getInputBufferSize();
            Long outputRowsValue = component.getOutputBufferSize();
            if (inputRowsValue != null && outputRowsValue != null) {
              long inputRows = inputRowsValue.longValue();
              long outputRows = outputRowsValue.longValue();

              // if the transform can't keep up with its input, mark it by drawing an animation
              boolean isSlow = inputRows * 0.85 > outputRows;
              if (isSlow) {
                gc.setLineWidth(lineWidth + 1);
                if (System.currentTimeMillis() % 2000 > 1000) {
                  gc.setForeground(EColor.BACKGROUND);
                  gc.setLineStyle(ELineStyle.SOLID);
                  gc.drawRectangle(x + 1, y + 1, iconSize - 2, iconSize - 2);

                  gc.setForeground(EColor.DARKGRAY);
                  gc.setLineStyle(ELineStyle.DOT);
                  gc.drawRectangle(x + 1, y + 1, iconSize - 2, iconSize - 2);
                } else {
                  gc.setForeground(EColor.DARKGRAY);
                  gc.setLineStyle(ELineStyle.SOLID);
                  gc.drawRectangle(x + 1, y + 1, iconSize - 2, iconSize - 2);

                  gc.setForeground(EColor.BACKGROUND);
                  gc.setLineStyle(ELineStyle.DOT);
                  gc.drawRectangle(x + 1, y + 1, iconSize - 2, iconSize - 2);
                }
              }
            }
          }
          gc.setLineStyle(ELineStyle.SOLID);
        }
      }
    }
  }

  private void drawTransformPerformanceTable(TransformMeta transformMeta) {

    if (transformMeta == null) {
      return;
    }

    // draw optional performance indicator
    if (pipeline != null) {

      Point pt = transformMeta.getLocation();
      if (pt == null) {
        pt = new Point(50, 50);
      }

      Point screen = real2screen(pt.x, pt.y);
      int x = screen.x;
      int y = screen.y;

      List<IEngineComponent> transforms = pipeline.getComponentCopies(transformMeta.getName());

      // draw mouse over performance indicator
      if (pipeline.isRunning()) {

        if (transformMeta.isSelected()) {

          // determine popup dimensions up front
          int popupX = x;
          int popupY = y;

          int popupWidth = 0;
          int popupHeight = 1;

          gc.setFont(EFont.SMALL);
          Point p = gc.textExtent("0000000000");
          int colWidth = p.x + MINI_ICON_MARGIN;
          int rowHeight = p.y + MINI_ICON_MARGIN;
          int titleWidth = 0;

          // calculate max title width to get the colum with
          String[] titles = PipelinePainter.getPeekTitles();

          for (String title : titles) {
            Point titleExtent = gc.textExtent(title);
            titleWidth = Math.max(titleExtent.x + MINI_ICON_MARGIN, titleWidth);
            popupHeight += titleExtent.y + MINI_ICON_MARGIN;
          }

          popupWidth = titleWidth + 2 * MINI_ICON_MARGIN;

          // determine total popup width
          popupWidth += transforms.size() * colWidth;

          // determine popup position
          popupX = popupX + (iconSize - popupWidth) / 2;
          popupY = popupY - popupHeight - MINI_ICON_MARGIN;

          // draw the frame
          gc.setForeground(EColor.DARKGRAY);
          gc.setBackground(EColor.LIGHTGRAY);
          gc.setLineWidth(1);
          gc.fillRoundRectangle(popupX, popupY, popupWidth, popupHeight, 7, 7);
          // draw the title columns
          gc.setBackground(EColor.LIGHTGRAY);
          gc.drawRoundRectangle(popupX, popupY, popupWidth, popupHeight, 7, 7);

          for (int i = 0, barY = popupY; i < titles.length; i++) {
            // fill each line with a slightly different background color

            if (i % 2 == 1) {
              gc.setBackground(EColor.BACKGROUND);
            } else {
              gc.setBackground(EColor.LIGHTGRAY);
            }
            gc.fillRoundRectangle(popupX + 1, barY + 1, popupWidth - 2, rowHeight, 7, 7);
            barY += rowHeight;
          }

          // draw the header column
          int rowY = popupY + MINI_ICON_MARGIN;
          int rowX = popupX + MINI_ICON_MARGIN;

          gc.setForeground(EColor.BLACK);
          gc.setBackground(EColor.BACKGROUND);

          for (int i = 0; i < titles.length; i++) {
            if (i % 2 == 1) {
              gc.setBackground(EColor.BACKGROUND);
            } else {
              gc.setBackground(EColor.LIGHTGRAY);
            }
            gc.drawText(titles[i], rowX, rowY);
            rowY += rowHeight;
          }

          // draw the values for each copy of the transform
          gc.setBackground(EColor.LIGHTGRAY);
          rowX += titleWidth;

          for (IEngineComponent transform : transforms) {

            rowX += colWidth;
            rowY = popupY + MINI_ICON_MARGIN;

            String[] fields = getPeekFields(transform);

            for (int i = 0; i < fields.length; i++) {
              if (i % 2 == 1) {
                gc.setBackground(EColor.BACKGROUND);
              } else {
                gc.setBackground(EColor.LIGHTGRAY);
              }
              drawTextRightAligned(fields[i], rowX, rowY);
              rowY += rowHeight;
            }
          }
        }
      }
    }
  }

  public String[] getPeekFields(IEngineComponent component) {

    long durationMs;
    String duration;
    Date firstRowReadDate = component.getFirstRowReadDate();
    if (firstRowReadDate != null) {
      durationMs = System.currentTimeMillis() - firstRowReadDate.getTime();
      duration = Utils.getDurationHMS(((double) durationMs) / 1000);
    } else {
      durationMs = 0;
      duration = "";
    }
    String speed;
    if (durationMs > 0) {
      // Look at the maximum read/written
      //
      long maxReadWritten = Math.max(component.getLinesRead(), component.getLinesWritten());
      long maxInputOutput = Math.max(component.getLinesInput(), component.getLinesOutput());
      long processed = Math.max(maxReadWritten, maxInputOutput);

      double durationSec = ((double) durationMs) / 1000.0;
      double rowsPerSec = ((double) processed) / durationSec;
      speed = new DecimalFormat("##,###,##0").format(rowsPerSec);
    } else {
      speed = "-";
    }

    boolean active = firstRowReadDate != null && component.getLastRowWrittenDate() == null;

    String[] fields =
        new String[] {
          Integer.toString(component.getCopyNr()),
          Long.toString(component.getLinesRead()),
          Long.toString(component.getLinesWritten()),
          Long.toString(component.getLinesInput()),
          Long.toString(component.getLinesOutput()),
          Long.toString(component.getLinesUpdated()),
          Long.toString(component.getLinesRejected()),
          Long.toString(component.getErrors()),
          active ? "Yes" : "No",
          duration,
          speed,
          component.getInputBufferSize() + "/" + component.getOutputBufferSize()
        };
    return fields;
  }

  private void drawTransformStatusIndicator(TransformMeta transformMeta) throws HopException {

    if (transformMeta == null) {
      return;
    }

    // draw status indicator
    if (pipeline != null) {

      Point pt = transformMeta.getLocation();
      if (pt == null) {
        pt = new Point(50, 50);
      }

      Point screen = real2screen(pt.x, pt.y);
      int x = screen.x;
      int y = screen.y;

      if (pipeline != null) {
        List<IEngineComponent> transforms = pipeline.getComponentCopies(transformMeta.getName());

        for (IEngineComponent transform : transforms) {
          String transformStatus = transform.getStatusDescription();
          if (transformStatus != null
              && transformStatus.equalsIgnoreCase(
                  EngineComponent.ComponentExecutionStatus.STATUS_FINISHED.getDescription())) {
            gc.drawImage(
                EImage.SUCCESS,
                (x + iconSize) - (miniIconSize / 2) + 1,
                y - (miniIconSize / 2) - 1,
                magnification);
          }
        }
      }
    }
  }

  private void drawTransformOutputIndicator(TransformMeta transformMeta) throws HopException {

    if (transformMeta == null) {
      return;
    }

    // draw status indicator
    if (pipeline != null) {

      Point pt = transformMeta.getLocation();
      if (pt == null) {
        pt = new Point(50, 50);
      }

      Point screen = real2screen(pt.x, pt.y);
      int x = screen.x;
      int y = screen.y;

      RowBuffer rowBuffer = outputRowsMap.get(transformMeta.getName());
      if (rowBuffer != null && !rowBuffer.isEmpty()) {
        int iconWidth = miniIconSize;
        int iconX = x + iconSize - (miniIconSize / 2) + 1;
        int iconY = y + iconSize - (miniIconSize / 2) + 1;
        gc.drawImage(EImage.DATA, iconX, iconY, magnification);
        areaOwners.add(
            new AreaOwner(
                AreaType.TRANSFORM_OUTPUT_DATA,
                iconX,
                iconY,
                iconWidth,
                iconWidth,
                offset,
                transformMeta,
                rowBuffer));
      }
    }
  }

  private void drawTextRightAligned(String txt, int x, int y) {
    int off = gc.textExtent(txt).x;
    x -= off;
    gc.drawText(txt, x, y);
  }

  private void drawHop(PipelineHopMeta hi) throws HopException {
    drawHop(hi, false);
  }

  private void drawHop(PipelineHopMeta hi, boolean isCandidate) throws HopException {
    TransformMeta fs = hi.getFromTransform();
    TransformMeta ts = hi.getToTransform();

    if (fs != null && ts != null) {
      drawLine(fs, ts, hi, isCandidate);
    }
  }

  private void drawTransform(TransformMeta transformMeta) throws HopException {
    if (transformMeta == null) {
      return;
    }
    boolean isDeprecated = transformMeta.isDeprecated();
    int alpha = gc.getAlpha();

    Point pt = transformMeta.getLocation();
    if (pt == null) {
      pt = new Point(50, 50);
    }

    Point screen = real2screen(pt.x, pt.y);
    int x = screen.x;
    int y = screen.y;

    boolean transformError = false;
    if (transformLogMap != null && !transformLogMap.isEmpty()) {
      String log = transformLogMap.get(transformMeta.getName());
      if (!Utils.isEmpty(log)) {
        transformError = true;
      }
    }

    // PARTITIONING

    // If this transform is partitioned, we're drawing a small symbol indicating this...
    //
    if (transformMeta.isPartitioned()) {
      gc.setLineWidth(1);
      gc.setForeground(EColor.MAGENTA);
      gc.setBackground(EColor.BACKGROUND);
      gc.setFont(EFont.GRAPH);

      PartitionSchema partitionSchema =
          transformMeta.getTransformPartitioningMeta().getPartitionSchema();
      if (partitionSchema != null) {
        String nrInput = "Px";
        if (partitionSchema.isDynamicallyDefined()) {
          nrInput += Const.NVL(partitionSchema.getNumberOfPartitions(), "?");
        } else {
          nrInput += partitionSchema.getPartitionIDs().size();
        }

        Point textExtent = gc.textExtent(nrInput);
        textExtent.x += 2; // add a tiny little bit of a margin
        textExtent.y += 2;

        // Draw it a 2 icons above the transform icon.
        // Draw it an icon and a half to the left
        //
        Point point = new Point(x - iconSize - iconSize / 2, y - iconSize - iconSize);
        gc.drawRectangle(point.x, point.y, textExtent.x, textExtent.y);
        gc.drawText(nrInput, point.x + 1, point.y + 1);

        // Now we draw an arrow from the rectangle to the transform...
        //
        gc.drawLine(
            point.x + textExtent.x / 2, point.y + textExtent.y, x + iconSize / 2, y + iconSize / 2);

        // Also draw the name of the partition schema below the box
        //
        gc.setForeground(EColor.PURPULE);
        gc.drawText(
            Const.NVL(partitionSchema.getName(), "<no partition name>"),
            point.x,
            point.y + textExtent.y + 3,
            true);

        // Add to the list of areas...
        //
        areaOwners.add(
            new AreaOwner(
                AreaType.TRANSFORM_PARTITIONING,
                point.x,
                point.y,
                textExtent.x,
                textExtent.y,
                offset,
                transformMeta,
                STRING_PARTITIONING_CURRENT_TRANSFORM));
      }
    }

    String name = transformMeta.getName();

    if (transformMeta.isSelected()) {
      gc.setLineWidth(lineWidth + 2);
    } else {
      gc.setLineWidth(lineWidth);
    }

    // Add to the list of areas...
    areaOwners.add(
        new AreaOwner(
            AreaType.TRANSFORM_ICON,
            x,
            y,
            iconSize,
            iconSize,
            offset,
            pipelineMeta,
            transformMeta));

    gc.setBackground(EColor.BACKGROUND);
    gc.fillRoundRectangle(x - 1, y - 1, iconSize + 1, iconSize + 1, 8, 8);
    gc.drawTransformIcon(x, y, transformMeta, magnification);
    if (transformError || transformMeta.isMissing()) {
      gc.setForeground(EColor.RED);
    } else if (isDeprecated) {
      gc.setForeground(EColor.DEPRECATED);
    } else {
      gc.setForeground(EColor.CRYSTAL);
    }
    gc.drawRoundRectangle(x - 1, y - 1, iconSize + 1, iconSize + 1, 8, 8);

    // Show an information icon in the upper left corner of the transform...
    //
    if (!Utils.isEmpty(transformMeta.getDescription())) {
      int xInfo = x - (miniIconSize / 2) - 1;
      int yInfo = y - (miniIconSize / 2) - 1;
      gc.drawImage(EImage.INFO_DISABLED, xInfo, yInfo, magnification);
      areaOwners.add(
          new AreaOwner(
              AreaType.TRANSFORM_INFO_ICON,
              xInfo,
              yInfo,
              miniIconSize,
              miniIconSize,
              offset,
              pipelineMeta,
              transformMeta));
    }

    Point namePosition = getNamePosition(name, screen, iconSize);
    Point nameExtent = gc.textExtent(name);

    // Help out the user working in single-click mode by allowing the name to be clicked to edit
    //
    if (isDrawingBorderAroundName()) {
      int tmpAlpha = gc.getAlpha();
      gc.setAlpha(230);
      gc.setBackground(EColor.LIGHTGRAY);
      gc.fillRoundRectangle(
          namePosition.x - 8,
          namePosition.y - 2,
          nameExtent.x + 15,
          nameExtent.y + 8,
          BasePainter.CORNER_RADIUS_5 + 15,
          BasePainter.CORNER_RADIUS_5 + 15);
      gc.setAlpha(tmpAlpha);
    }

    // Add the area owner for the transform name
    //
    areaOwners.add(
        new AreaOwner(
            AreaType.TRANSFORM_NAME,
            namePosition.x - 8,
            namePosition.y - 2,
            nameExtent.x + 15,
            nameExtent.y + 8,
            offset,
            transformMeta,
            name));

    gc.setForeground(EColor.BLACK);
    gc.setFont(EFont.GRAPH);
    gc.drawText(name, namePosition.x, namePosition.y + 2, true);
    boolean partitioned = false;

    // See if we need to draw a line under the name to make the name look like a hyperlink.
    //
    if (name.equals(mouseOverName)) {
      gc.setLineWidth(lineWidth);      
      gc.drawLine(
          namePosition.x,
          namePosition.y + nameExtent.y,
          namePosition.x + nameExtent.x ,
          namePosition.y + nameExtent.y);
    }

    TransformPartitioningMeta meta = transformMeta.getTransformPartitioningMeta();
    if (transformMeta.isPartitioned() && meta != null) {
      partitioned = true;
    }

    if (!transformMeta.getCopiesString().equals("1") && !partitioned) {
      gc.setBackground(EColor.BACKGROUND);
      gc.setForeground(EColor.BLACK);
      String copies = "x" + transformMeta.getCopiesString();
      Point textExtent = gc.textExtent(copies);

      gc.drawText(copies, x - textExtent.x + 1, y - textExtent.y - 4, false);
      areaOwners.add(
          new AreaOwner(
              AreaType.TRANSFORM_COPIES_TEXT,
              x - textExtent.x + 1,
              y - textExtent.y - 4,
              textExtent.x,
              textExtent.y,
              offset,
              pipelineMeta,
              transformMeta));
    }

    // If there was an error during the run, the map "transformLogMap" is not empty and not null.
    //
    if (transformError) {
      String log = transformLogMap.get(transformMeta.getName());

      // Show an error lines icon in the upper right corner of the transform...
      //
      int xError = (x + iconSize) - (miniIconSize / 2) + 1;
      int yError = y - (miniIconSize / 2) - 1;
      gc.drawImage(EImage.FAILURE, xError, yError, magnification);

      areaOwners.add(
          new AreaOwner(
              AreaType.TRANSFORM_FAILURE_ICON,
              xError,
              yError,
              16,
              16,
              offset,
              log,
              STRING_TRANSFORM_ERROR_LOG));
    }

    PipelinePainterExtension extension =
        new PipelinePainterExtension(
            gc,
            areaOwners,
            pipelineMeta,
            transformMeta,
            null,
            x,
            y,
            0,
            0,
            0,
            0,
            offset,
            iconSize,
            stateMap);
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelinePainterTransform.id, extension);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error calling extension point(s) for the pipeline painter transform", e);
    }

    // Restore the previous alpha value
    //
    gc.setAlpha(alpha);
  }

  public Point getNamePosition(String string, Point screen, int iconsize) {
    Point textsize = gc.textExtent(string);

    int xpos = screen.x + (iconsize / 2) - (textsize.x / 2);
    int ypos = screen.y + iconsize + 5;

    return new Point(xpos, ypos);
  }

  private void drawLine(TransformMeta fs, TransformMeta ts, PipelineHopMeta hi, boolean isCandidate)
      throws HopException {
    int[] line = getLine(fs, ts);

    EColor color;
    ELineStyle linestyle = ELineStyle.SOLID;
    int activeLinewidth = lineWidth;

    EImage arrow;
    if (isCandidate) {
      color = EColor.BLUE;
      arrow = EImage.ARROW_CANDIDATE;
    } else {
      if (hi.isEnabled()) {
        if (fs.isSendingErrorRowsToTransform(ts)) {
          color = EColor.RED;
          linestyle = ELineStyle.DASH;
          arrow = EImage.ARROW_ERROR;
        } else {
          color = EColor.HOP_DEFAULT;
          arrow = EImage.ARROW_DEFAULT;
        }

        ITransformIOMeta ioMeta = fs.getTransform().getTransformIOMeta();
        IStream targetStream = ioMeta.findTargetStream(ts);

        if (targetStream != null) {
          if (targetStream.getStreamIcon() == StreamIcon.TRUE) {
            color = EColor.HOP_TRUE;
            arrow = EImage.ARROW_TRUE;
          } else if (targetStream.getStreamIcon() == StreamIcon.FALSE) {
            color = EColor.HOP_FALSE;
            arrow = EImage.ARROW_FALSE;
          }
        }
      } else {
        color = EColor.GRAY;
        arrow = EImage.ARROW_DISABLED;
      }
    }
    if (hi.isSplit()) {
      activeLinewidth = lineWidth + 2;
    }

    // Check to see if the source transform is an info transform for the target transform.
    //
    ITransformIOMeta ioMeta = ts.getTransform().getTransformIOMeta();
    List<IStream> infoStreams = ioMeta.getInfoStreams();
    if (!infoStreams.isEmpty()) {
      // Check this situation, the source transform can't run in multiple copies!
      //
      for (IStream stream : infoStreams) {
        if (fs.getName().equalsIgnoreCase(stream.getTransformName())) {
          // This is the info transform over this hop!
          //

          // Only valid if both transforms are partitioned
          //
          if (fs.isPartitioned() && ts.isPartitioned()) {
            //
          } else if (fs.getCopies(variables) > 1) {
            // This is not a desirable situation, it will always end in error.
            // As such, it's better not to give feedback on it.
            // We do this by drawing an error icon over the hop...
            //
            color = EColor.RED;
            arrow = EImage.ARROW_ERROR;
          }
        }
      }
    }

    gc.setForeground(color);
    gc.setLineStyle(linestyle);
    gc.setLineWidth(activeLinewidth);

    drawArrow(arrow, line, hi, fs, ts);

    if (hi.isSplit()) {
      gc.setLineWidth(lineWidth);
    }

    gc.setForeground(EColor.BLACK);
    gc.setBackground(EColor.BACKGROUND);
    gc.setLineStyle(ELineStyle.SOLID);
  }

  @Override
  protected void drawArrow(
      EImage arrow,
      int x1,
      int y1,
      int x2,
      int y2,
      double theta,
      int size,
      double factor,
      PipelineHopMeta pipelineHop,
      Object startObject,
      Object endObject)
      throws HopException {
    int mx;
    int my;
    int a;
    int b;
    int dist;
    double angle;

    gc.drawLine(x1, y1, x2, y2);

    // What's the distance between the 2 points?
    a = Math.abs(x2 - x1);
    b = Math.abs(y2 - y1);
    dist = (int) Math.sqrt(a * a + b * b);

    // determine factor (position of arrow to left side or right side
    // 0-->100%)
    if (factor < 0) {
      if (dist >= 2 * iconSize) {
        factor = 1.3;
      } else {
        factor = 1.2;
      }
    }

    // in between 2 points
    mx = (int) (x1 + factor * (x2 - x1) / 2);
    my = (int) (y1 + factor * (y2 - y1) / 2);

    // calculate points for arrowhead
    angle = Math.atan2(y2 - y1, x2 - x1) + (Math.PI / 2);

    boolean q1 = Math.toDegrees(angle) >= 0 && Math.toDegrees(angle) < 90;
    boolean q2 = Math.toDegrees(angle) >= 90 && Math.toDegrees(angle) < 180;
    boolean q3 = Math.toDegrees(angle) >= 180 && Math.toDegrees(angle) < 270;
    boolean q4 = Math.toDegrees(angle) >= 270 || Math.toDegrees(angle) < 0;

    if (q1 || q3) {
      gc.drawImage(arrow, mx, my + 1, magnification, angle);
    } else if (q2 || q4) {
      gc.drawImage(arrow, mx, my, magnification, angle);
    }

    if (startObject instanceof TransformMeta && endObject instanceof TransformMeta) {
      factor = 0.8;

      TransformMeta fs = (TransformMeta) startObject;
      TransformMeta ts = (TransformMeta) endObject;

      // in between 2 points
      mx = (int) (x1 + factor * (x2 - x1) / 2) - miniIconSize / 2;
      my = (int) (y1 + factor * (y2 - y1) / 2) - miniIconSize / 2;

      boolean errorHop =
          fs.isSendingErrorRowsToTransform(ts)
              || (startErrorHopTransform && fs.equals(startHopTransform));
      boolean targetHop =
          Const.indexOfString(
                  ts.getName(), fs.getTransform().getTransformIOMeta().getTargetTransformNames())
              >= 0;

      if (targetHop) {
        ITransformIOMeta ioMeta = fs.getTransform().getTransformIOMeta();
        IStream targetStream = ioMeta.findTargetStream(ts);
        if (targetStream != null) {
          EImage image =
              BasePainter.getStreamIconImage(targetStream.getStreamIcon(), pipelineHop.isEnabled());
          gc.drawImage(image, mx, my, magnification);

          areaOwners.add(
              new AreaOwner(
                  AreaType.TRANSFORM_TARGET_HOP_ICON, mx, my, 16, 16, offset, fs, targetStream));
        }
      } else if (fs.isDistributes()
          && fs.getRowDistribution() != null
          && !ts.getTransformPartitioningMeta().isMethodMirror()
          && !errorHop) {

        // Draw the custom row distribution plugin icon
        //
        SvgFile svgFile = fs.getRowDistribution().getDistributionImage();
        if (svgFile != null) {
          //
          gc.drawImage(svgFile, mx, my, 16, 16, magnification, 0);
          areaOwners.add(
              new AreaOwner(
                  AreaType.ROW_DISTRIBUTION_ICON,
                  mx,
                  my,
                  16,
                  16,
                  offset,
                  fs,
                  STRING_ROW_DISTRIBUTION));
          mx += 16;
        }

      } else if (!fs.isDistributes()
          && !ts.getTransformPartitioningMeta().isMethodMirror()
          && !errorHop) {

        // Draw the copy icon on the hop
        //
        EImage image = (pipelineHop.isEnabled()) ? EImage.COPY_ROWS : EImage.COPY_ROWS_DISABLED;
        gc.drawImage(image, mx, my, magnification);

        areaOwners.add(
            new AreaOwner(
                AreaType.HOP_COPY_ICON, mx, my, 16, 16, offset, fs, STRING_HOP_TYPE_COPY));
        mx += 16;
      }

      if (errorHop) {
        EImage image = (pipelineHop.isEnabled()) ? EImage.ERROR : EImage.ERROR_DISABLED;
        gc.drawImage(image, mx, my, magnification);
        areaOwners.add(new AreaOwner(AreaType.HOP_ERROR_ICON, mx, my, 16, 16, offset, fs, ts));
        mx += 16;
      }

      ITransformIOMeta ioMeta = ts.getTransform().getTransformIOMeta();
      String[] infoTransformNames = ioMeta.getInfoTransformNames();

      if ((candidateHopType == StreamType.INFO
              && ts.equals(endHopTransform)
              && fs.equals(startHopTransform))
          || Const.indexOfString(fs.getName(), infoTransformNames) >= 0) {
        EImage image = (pipelineHop.isEnabled()) ? EImage.INFO : EImage.INFO_DISABLED;
        gc.drawImage(image, mx, my, magnification);
        areaOwners.add(new AreaOwner(AreaType.HOP_INFO_ICON, mx, my, 16, 16, offset, fs, ts));
        mx += 16;
      }

      // Check to see if the source transform is an info transform for the target transform.
      //
      if (!Utils.isEmpty(infoTransformNames)) {
        // Check this situation, the source transform can't run in multiple copies!
        //
        for (String infoTransform : infoTransformNames) {
          if (fs.getName().equalsIgnoreCase(infoTransform)) {
            // This is the info transform over this hop!
            //
            // Only valid if both transforms are partitioned
            //
            if (fs.isPartitioned() && ts.isPartitioned()) {
              // TODO explain in the UI what's going on.
              //
              gc.drawImage(EImage.PARALLEL, mx, my, magnification);
              areaOwners.add(
                  new AreaOwner(
                      AreaType.HOP_INFO_TRANSFORMS_PARTITIONED,
                      mx,
                      my,
                      miniIconSize,
                      miniIconSize,
                      offset,
                      fs,
                      ts));
              mx += 16;
            } else if (fs.getCopies(variables) > 1) {
              // This is not a desirable situation, it will always end in error.
              // As such, it's better not to give feedback on it.
              // We do this by drawing an error icon over the hop...
              //
              gc.drawImage(EImage.ERROR, mx, my, magnification);
              areaOwners.add(
                  new AreaOwner(
                      AreaType.HOP_INFO_TRANSFORM_COPIES_ERROR,
                      mx,
                      my,
                      miniIconSize,
                      miniIconSize,
                      offset,
                      fs,
                      ts));
              mx += 16;
            }
          }
        }
      }
    }

    PipelinePainterExtension extension =
        new PipelinePainterExtension(
            gc,
            areaOwners,
            pipelineMeta,
            null,
            pipelineHop,
            x1,
            y1,
            x2,
            y2,
            mx,
            my,
            offset,
            iconSize,
            stateMap);
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelinePainterArrow.id, extension);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error calling extension point(s) for the pipeline painter arrow", e);
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
  public void setTransformLogMap(Map<String, String> transformLogMap) {
    this.transformLogMap = transformLogMap;
  }

  /**
   * @param startHopTransform the start Hop Transform to set
   */
  public void setStartHopTransform(TransformMeta startHopTransform) {
    this.startHopTransform = startHopTransform;
  }

  /**
   * @param endHopLocation the endHopLocation to set
   */
  public void setEndHopLocation(Point endHopLocation) {
    this.endHopLocation = endHopLocation;
  }

  /**
   * @param noInputTransform the no Input Transform to set
   */
  public void setNoInputTransform(TransformMeta noInputTransform) {
    this.noInputTransform = noInputTransform;
  }

  /**
   * @param endHopTransform the end Hop Transform to set
   */
  public void setEndHopTransform(TransformMeta endHopTransform) {
    this.endHopTransform = endHopTransform;
  }

  public void setCandidateHopType(StreamType candidateHopType) {
    this.candidateHopType = candidateHopType;
  }

  public void setStartErrorHopTransform(boolean startErrorHopTransform) {
    this.startErrorHopTransform = startErrorHopTransform;
  }

  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  public void setPipelineMeta(PipelineMeta pipelineMeta) {
    this.pipelineMeta = pipelineMeta;
  }

  public IPipelineEngine<PipelineMeta> getPipeline() {
    return pipeline;
  }

  public void setPipeline(IPipelineEngine<PipelineMeta> pipeline) {
    this.pipeline = pipeline;
  }

  public boolean isSlowTransformIndicatorEnabled() {
    return slowTransformIndicatorEnabled;
  }

  public void setSlowTransformIndicatorEnabled(boolean slowTransformIndicatorEnabled) {
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

  /**
   * Gets outputRowsMap
   *
   * @return value of outputRowsMap
   */
  public Map<String, RowBuffer> getOutputRowsMap() {
    return outputRowsMap;
  }

  /**
   * @param outputRowsMap The outputRowsMap to set
   */
  public void setOutputRowsMap(Map<String, RowBuffer> outputRowsMap) {
    this.outputRowsMap = outputRowsMap;
  }

  /**
   * Gets stateMap
   *
   * @return value of stateMap
   */
  public Map<String, Object> getStateMap() {
    return stateMap;
  }

  /**
   * @param stateMap The stateMap to set
   */
  public void setStateMap(Map<String, Object> stateMap) {
    this.stateMap = stateMap;
  }
}
