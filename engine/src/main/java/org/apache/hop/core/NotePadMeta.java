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

package org.apache.hop.core;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.IGuiPosition;
import org.apache.hop.core.gui.IGuiSize;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.w3c.dom.Node;

/** Describes a note displayed on a Pipeline or Workflow. */
public class NotePadMeta implements Cloneable, IGuiPosition, IGuiSize {
  public static final String XML_TAG = "notepad";

  public static final int COLOR_RGB_BLACK_RED = 14;
  public static final int COLOR_RGB_BLACK_GREEN = 58;
  public static final int COLOR_RGB_BLACK_BLUE = 90;

  public static final int COLOR_RGB_DEFAULT_BG_RED = 201;
  public static final int COLOR_RGB_DEFAULT_BG_GREEN = 232;
  public static final int COLOR_RGB_DEFAULT_BG_BLUE = 251;

  public static final int COLOR_RGB_DEFAULT_BORDER_RED = 14;
  public static final int COLOR_RGB_DEFAULT_BORDER_GREEN = 58;
  public static final int COLOR_RGB_DEFAULT_BORDER_BLUE = 90;

  @Setter @Getter @HopMetadataProperty private String note;

  @Setter
  @Getter
  @HopMetadataProperty(key = "fontname")
  private String fontName;

  @Setter
  @Getter
  @HopMetadataProperty(key = "fontsize")
  private int fontSize;

  @Getter
  @Setter
  @HopMetadataProperty(key = "fontbold")
  private boolean fontBold;

  @Getter
  @Setter
  @HopMetadataProperty(key = "fontitalic")
  private boolean fontItalic;

  @Getter
  @Setter
  @HopMetadataProperty(key = "fontcolorred")
  private int fontColorRed;

  @Getter
  @Setter
  @HopMetadataProperty(key = "fontcolorgreen")
  private int fontColorGreen;

  @Getter
  @Setter
  @HopMetadataProperty(key = "fontcolorblue")
  private int fontColorBlue;

  @Getter
  @Setter
  @HopMetadataProperty(key = "backgroundcolorred")
  private int backGroundColorRed;

  @Getter
  @Setter
  @HopMetadataProperty(key = "backgroundcolorgreen")
  private int backGroundColorGreen;

  @Getter
  @Setter
  @HopMetadataProperty(key = "backgroundcolorblue")
  private int backGroundColorBlue;

  @Getter
  @Setter
  @HopMetadataProperty(key = "bordercolorred")
  private int borderColorRed;

  @Getter
  @Setter
  @HopMetadataProperty(key = "bordercolorgreen")
  private int borderColorGreen;

  @Getter
  @Setter
  @HopMetadataProperty(key = "bordercolorblue")
  private int borderColorBlue;

  @HopMetadataProperty(inline = true)
  private Point location;

  @HopMetadataProperty public int width;
  @HopMetadataProperty public int height;

  @Getter @Setter private int minimumWidth;
  @Getter @Setter private int minimumHeight;

  private boolean selected;
  private boolean changed;

  public NotePadMeta() {
    this.note = null;
    this.location = new Point(-1, -1);
    this.width = -1;
    this.height = -1;
    this.selected = false;

    setDefaultFont();
  }

  public NotePadMeta(String note, int locationX, int locationY, int width, int height) {
    this();
    this.note = note;
    this.location = new Point(locationX, locationY);
    this.width = width;
    this.height = height;
  }

  public NotePadMeta(
      String note,
      int x,
      int y,
      int width,
      int height,
      String fontName,
      int fontSize,
      boolean fontBold,
      boolean fontItalic,
      int fontColorRed,
      int fontColorGreen,
      int fontColorBlue,
      int backGroundColorRed,
      int backGroundColorGreen,
      int backGroundColorBlue,
      int borderColorRed,
      int borderColorGreen,
      int borderColorBlue) {
    this.note = note;
    this.location = new Point(x, y);
    this.width = width;
    this.height = height;
    this.selected = false;
    this.fontName = fontName;
    this.fontSize = fontSize;
    this.fontBold = fontBold;
    this.fontItalic = fontItalic;
    // font color
    this.fontColorRed = fontColorRed;
    this.fontColorGreen = fontColorGreen;
    this.fontColorBlue = fontColorBlue;
    // background color
    this.backGroundColorRed = backGroundColorRed;
    this.backGroundColorGreen = backGroundColorGreen;
    this.backGroundColorBlue = backGroundColorBlue;
    // border color
    this.borderColorRed = borderColorRed;
    this.borderColorGreen = borderColorGreen;
    this.borderColorBlue = borderColorBlue;
  }

  public NotePadMeta(NotePadMeta n) {
    this(
        n.note,
        n.location.x,
        n.location.y,
        n.width,
        n.height,
        n.fontName,
        n.fontSize,
        n.fontBold,
        n.fontItalic,
        n.fontColorRed,
        n.fontColorGreen,
        n.fontColorBlue,
        n.backGroundColorRed,
        n.backGroundColorGreen,
        n.backGroundColorBlue,
        n.borderColorRed,
        n.borderColorGreen,
        n.borderColorBlue);
  }

  public NotePadMeta(Node nodePadNode) throws HopXmlException {
    this();
    try {
      // De-serialize using metadata properties
      //
      XmlMetadataUtil.deSerializeFromXml(nodePadNode, NotePadMeta.class, this, null);
    } catch (Exception e) {
      throw new HopXmlException("Unable to read Notepad metadata from XML", e);
    }
  }

  public String getXml() {
    try {
      return XmlHandler.openTag(XML_TAG)
          + XmlMetadataUtil.serializeObjectToXml(this)
          + XmlHandler.closeTag(XML_TAG);
    } catch (Exception e) {
      throw new RuntimeException("Error serializing notepad metadata to XML", e);
    }
  }

  @Override
  public void setLocation(int x, int y) {
    if (x != location.x || y != location.y) {
      setChanged();
    }
    location.x = x;
    location.y = y;
  }

  @Override
  public void setLocation(Point point) {
    if (point != null) {
      setLocation(point.x, point.y);
    } else {
      this.location = null;
    }
  }

  @Override
  public Point getLocation() {
    return location;
  }

  /**
   * @return Returns the selected.
   */
  @Override
  public boolean isSelected() {
    return selected;
  }

  /**
   * @param selected The selected to set.
   */
  @Override
  public void setSelected(boolean selected) {
    this.selected = selected;
  }

  /** Change a selected state to not-selected and vice-versa. */
  public void flipSelected() {
    this.selected = !this.selected;
  }

  @Override
  public NotePadMeta clone() {
    return new NotePadMeta(this);
  }

  public void setChanged() {
    setChanged(true);
  }

  public void setChanged(boolean ch) {
    changed = ch;
  }

  public boolean hasChanged() {
    return changed;
  }

  public String toString() {
    return note;
  }

  /**
   * @return the height
   */
  @Override
  public int getHeight() {
    return height;
  }

  /**
   * @param height the height to set
   */
  @Override
  public void setHeight(int height) {
    if (this.height != height) {
      setChanged();
    }
    this.height = height;
  }

  /**
   * @return the width
   */
  @Override
  public int getWidth() {
    return width;
  }

  /**
   * @param width the width to set
   */
  @Override
  public void setWidth(int width) {
    if (this.width != width) {
      setChanged();
    }
    this.width = width;
  }

  private void setDefaultFont() {
    this.fontName = null;
    this.fontSize = -1;
    this.fontBold = false;
    this.fontItalic = false;

    // font color black
    this.fontColorRed = COLOR_RGB_BLACK_RED;
    this.fontColorGreen = COLOR_RGB_BLACK_GREEN;
    this.fontColorBlue = COLOR_RGB_BLACK_BLUE;

    // background yellow
    this.backGroundColorRed = COLOR_RGB_DEFAULT_BG_RED;
    this.backGroundColorGreen = COLOR_RGB_DEFAULT_BG_GREEN;
    this.backGroundColorBlue = COLOR_RGB_DEFAULT_BG_BLUE;

    // border gray
    this.borderColorRed = COLOR_RGB_DEFAULT_BORDER_RED;
    this.borderColorGreen = COLOR_RGB_DEFAULT_BORDER_GREEN;
    this.borderColorBlue = COLOR_RGB_DEFAULT_BORDER_BLUE;
  }
}
