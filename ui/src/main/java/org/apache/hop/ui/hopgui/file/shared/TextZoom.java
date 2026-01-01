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

package org.apache.hop.ui.hopgui.file.shared;

import org.apache.hop.ui.core.widget.TextComposite;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;

/**
 * Utility class to handle font zooming for text widgets. Supports both Text and StyledText widgets
 * for RAP/RWT compatibility.
 */
public class TextZoom {
  public static final int STEP_DEFAULT = 1;
  public static final int MIN_HEIGHT = 4;

  private Control widget;
  private Font font;
  private int step;
  private int minHeight;

  public TextZoom(Text widget, Font font) {
    this.widget = (Control) widget;
    this.font = font;
    this.step = TextZoom.STEP_DEFAULT;
    this.minHeight = MIN_HEIGHT;
  }

  public TextZoom(Text widget, Font font, int step) {
    this.widget = (Control) widget;
    this.font = font;
    this.step = step;
    this.minHeight = MIN_HEIGHT;
  }

  public TextZoom(Text widget, Font font, int step, int minHeight) {
    this.widget = (Control) widget;
    this.font = font;
    this.step = step;
    this.minHeight = minHeight;
  }

  /**
   * Constructor that accepts TextComposite (works for both Text and StyledText without direct
   * StyledText dependency).
   */
  public TextZoom(TextComposite widget, Font font) {
    this.widget = widget;
    this.font = font;
    this.step = TextZoom.STEP_DEFAULT;
    this.minHeight = MIN_HEIGHT;
  }

  /** Constructor that accepts TextComposite with custom step. */
  public TextZoom(TextComposite widget, Font font, int step) {
    this.widget = widget;
    this.font = font;
    this.step = step;
    this.minHeight = MIN_HEIGHT;
  }

  /** Constructor that accepts TextComposite with custom step and minHeight. */
  public TextZoom(TextComposite widget, Font font, int step, int minHeight) {
    this.widget = widget;
    this.font = font;
    this.step = step;
    this.minHeight = minHeight;
  }

  public void increaseFont() {
    increaseHeightBy(step);
  }

  public void decreaseFont() {
    increaseHeightBy(-step);
  }

  public void resetFont() {
    widget.setFont(font);
  }

  private void increaseHeightBy(int points) {
    FontData fontData = widget.getFont().getFontData()[0];
    int newHeight = Math.max(minHeight, fontData.getHeight() + points);
    fontData.setHeight(newHeight);
    widget.setFont(new Font(widget.getDisplay(), fontData));
  }
}
