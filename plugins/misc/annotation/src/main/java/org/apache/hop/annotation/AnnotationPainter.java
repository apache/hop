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

package org.apache.hop.annotation;

import java.util.List;
import org.apache.hop.annotation.AnnotationGeometry.Bounds;
import org.apache.hop.core.NotePadType;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.NotePadStyle;
import org.apache.hop.core.gui.NotePadStyle.RgbColor;

/** Draws one annotation callout and registers its click area. */
public final class AnnotationPainter {
  private AnnotationPainter() {}

  /**
   * Draws the annotation next to the icon. A blank annotation draws nothing. Colors come from the
   * note General palette, which follows {@link NotePadStyle#isDarkMode()}.
   *
   * @param gc canvas graphics
   * @param areaOwners areas hit-testing reads
   * @param offset canvas scroll offset subtracted from the click area
   * @param subject transform or action the callout belongs to
   * @param iconX icon left, in the same space {@code gc} is drawing in
   * @param iconY icon top
   * @param iconSize icon width and height
   * @param selected true when the owner is selected
   * @param annotation the annotation, or null
   */
  public static void paint(
      IGc gc,
      List<AreaOwner> areaOwners,
      DPoint offset,
      Object subject,
      int iconX,
      int iconY,
      int iconSize,
      boolean selected,
      Annotation annotation) {
    if (gc == null || areaOwners == null || subject == null || annotation == null) {
      return;
    }
    String text = annotation.getText();
    if (text == null || text.isBlank()) {
      return;
    }

    gc.setFont(IGc.EFont.NOTE);
    // Unscaled pixels. setTransform already scales what is drawn, and textExtent measures in that
    // same space. The click area below is this rectangle; screen2real converts the mouse into it.
    float magnification = gc.getMagnification();
    int maxWidth = AnnotationGeometry.MAX_TEXT_WIDTH_PX;
    List<String> lines = AnnotationGeometry.wrap(text, maxWidth, line -> gc.textExtent(line).x);
    if (lines.isEmpty()) {
      return;
    }

    int textWidth = 0;
    for (String line : lines) {
      textWidth = Math.max(textWidth, gc.textExtent(line).x);
    }
    int lineHeight = gc.getFontHeight();
    if (lineHeight <= 0) {
      lineHeight = Math.max(1, gc.textExtent("Ag").y);
    }
    int textHeight = lines.size() * lineHeight;
    Bounds icon = new Bounds(iconX, iconY, iconSize, iconSize);
    Bounds balloon =
        AnnotationGeometry.balloon(
            icon, textWidth, textHeight, annotation.getPlacement(), magnification);

    RgbColor background = NotePadStyle.backgroundColor(NotePadType.GENERAL);
    RgbColor border = NotePadStyle.borderColor(NotePadType.GENERAL);
    RgbColor foreground = NotePadStyle.textColor(NotePadType.GENERAL);
    int radius = AnnotationGeometry.RADIUS_PX;

    if (annotation.isDashedLine()) {
      AnnotationGeometry.Segment connector = AnnotationGeometry.connector(balloon, icon);
      gc.setForeground(border.red(), border.green(), border.blue());
      gc.setLineWidth(1);
      gc.setLineStyle(IGc.ELineStyle.DASH);
      gc.drawLine(connector.x1(), connector.y1(), connector.x2(), connector.y2());
      gc.setLineStyle(IGc.ELineStyle.SOLID);
    }

    gc.setBackground(background.red(), background.green(), background.blue());
    gc.setForeground(border.red(), border.green(), border.blue());
    gc.setLineWidth(NotePadStyle.borderWidth(NotePadType.GENERAL, selected));
    gc.fillRoundRectangle(
        balloon.x(), balloon.y(), balloon.width(), balloon.height(), radius, radius);
    gc.drawRoundRectangle(
        balloon.x(), balloon.y(), balloon.width(), balloon.height(), radius, radius);

    gc.setForeground(foreground.red(), foreground.green(), foreground.blue());
    int padding = AnnotationGeometry.PADDING_PX;
    int textX = balloon.x() + padding;
    int textY = balloon.y() + padding;
    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i);
      if (!line.isEmpty()) {
        gc.drawText(line, textX, textY + i * lineHeight, true);
      }
    }
    gc.setLineWidth(1);

    DPoint areaOffset = offset == null ? new DPoint(0, 0) : offset;
    areaOwners.add(
        new AreaOwner(
            AreaOwner.AreaType.CUSTOM,
            balloon.x(),
            balloon.y(),
            balloon.width(),
            balloon.height(),
            areaOffset,
            new AnnotationHit(subject),
            subject));
  }
}
