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

package org.apache.hop.core;

import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.IGuiPosition;
import org.apache.hop.core.gui.IGuiSize;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

/**
 * Describes a note displayed on a Pipeline, Workflow, Schema, or Report.
 *
 * @author Matt
 * @since 28-11-2003
 */
public class NotePadMeta implements Cloneable, IXml, IGuiPosition, IGuiSize {
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

  private String note;
  private String fontName;
  private int fontSize;
  private boolean fontBold;
  private boolean fontItalic;

  private int fontColorRed;
  private int fontColorGreen;
  private int fontColorBlue;

  private int backgroundColorRed;
  private int backgroundColorGreen;
  private int backgroundColorBlue;

  private int borderColorRed;
  private int borderColorGreen;
  private int borderColorBlue;

  private Point location;
  public int width, height;
  private boolean selected;

  private boolean changed;

  public NotePadMeta() {
    this.note = null;
    this.location = new Point( -1, -1 );
    this.width = -1;
    this.height = -1;
    this.selected = false;

    this.backgroundColorRed = COLOR_RGB_DEFAULT_BG_RED;
    this.backgroundColorGreen = COLOR_RGB_DEFAULT_BG_GREEN;
    this.backgroundColorBlue = COLOR_RGB_DEFAULT_BG_BLUE;

    setDefaultFont();
  }

  public NotePadMeta( String note, int locationX, int locationY, int width, int height ) {
    this();
    this.note = note;
    this.location = new Point( locationX, locationY );
    this.width = width;
    this.height = height;
  }

  public NotePadMeta( String n, int xl, int yl, int w, int h, String fontName, int fontSize, boolean fontBold,
                      boolean fontItalic, int fontColorRed, int fontColorGreen, int fontColorBlue, int backGrounColorRed,
                      int backGrounColorGreen, int backGrounColorBlue, int borderColorRed, int borderColorGreen,
                      int borderColorBlue ) {
    this.note = n;
    this.location = new Point( xl, yl );
    this.width = w;
    this.height = h;
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
    this.backgroundColorRed = backGrounColorRed;
    this.backgroundColorGreen = backGrounColorGreen;
    this.backgroundColorBlue = backGrounColorBlue;
    // border color
    this.borderColorRed = borderColorRed;
    this.borderColorGreen = borderColorGreen;
    this.borderColorBlue = borderColorBlue;
  }

  public NotePadMeta( NotePadMeta n ) {
    this( n.note, n.location.x, n.location.y, n.width, n.height, n.fontName, n.fontSize, n.fontBold, n.fontItalic,
      n.fontColorRed, n.fontColorGreen, n.fontColorBlue, n.backgroundColorRed,
      n.backgroundColorGreen, n.backgroundColorBlue, n.borderColorRed, n.borderColorGreen, n.borderColorBlue );
  }

  public NotePadMeta( Node notepadnode ) throws HopXmlException {
    try {
      note = XmlHandler.getTagValue( notepadnode, "note" );
      String sxloc = XmlHandler.getTagValue( notepadnode, "xloc" );
      String syloc = XmlHandler.getTagValue( notepadnode, "yloc" );
      String swidth = XmlHandler.getTagValue( notepadnode, "width" );
      String sheight = XmlHandler.getTagValue( notepadnode, "heigth" );
      int x = Const.toInt( sxloc, 0 );
      int y = Const.toInt( syloc, 0 );
      this.location = new Point( x, y );
      this.width = Const.toInt( swidth, 0 );
      this.height = Const.toInt( sheight, 0 );
      this.selected = false;
      this.fontName = XmlHandler.getTagValue( notepadnode, "fontname" );
      this.fontSize = Const.toInt( XmlHandler.getTagValue( notepadnode, "fontsize" ), -1 );
      this.fontBold = "Y".equalsIgnoreCase( XmlHandler.getTagValue( notepadnode, "fontbold" ) );
      this.fontItalic = "Y".equalsIgnoreCase( XmlHandler.getTagValue( notepadnode, "fontitalic" ) );
      // font color
      this.fontColorRed = Const.toInt( XmlHandler.getTagValue( notepadnode, "fontcolorred" ), COLOR_RGB_BLACK_RED );
      this.fontColorGreen =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "fontcolorgreen" ), COLOR_RGB_BLACK_GREEN );
      this.fontColorBlue =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "fontcolorblue" ), COLOR_RGB_BLACK_BLUE );
      // background color
      this.backgroundColorRed =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "backgroundcolorred" ), COLOR_RGB_DEFAULT_BG_RED );
      this.backgroundColorGreen =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "backgroundcolorgreen" ), COLOR_RGB_DEFAULT_BG_GREEN );
      this.backgroundColorBlue =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "backgroundcolorblue" ), COLOR_RGB_DEFAULT_BG_BLUE );
      // border color
      this.borderColorRed =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "bordercolorred" ), COLOR_RGB_DEFAULT_BORDER_RED );
      this.borderColorGreen =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "bordercolorgreen" ), COLOR_RGB_DEFAULT_BORDER_GREEN );
      this.borderColorBlue =
        Const.toInt( XmlHandler.getTagValue( notepadnode, "bordercolorblue" ), COLOR_RGB_DEFAULT_BORDER_BLUE );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to read Notepad info from XML", e );
    }
  }

  public void setLocation( int x, int y ) {
    if ( x != location.x || y != location.y ) {
      setChanged();
    }
    location.x = x;
    location.y = y;
  }

  public void setLocation( Point p ) {
    setLocation( p.x, p.y );
  }

  public Point getLocation() {
    return location;
  }

  /**
   * @return Returns the note.
   */
  public String getNote() {
    return this.note;
  }

  /**
   * @param note The note to set.
   */
  public void setNote( String note ) {
    this.note = note;
  }

  /**
   * @param red the border red color.
   */
  public void setBorderColorRed( int red ) {
    this.borderColorRed = red;
  }

  /**
   * @param green the border color green.
   */
  public void setBorderColorGreen( int green ) {
    this.borderColorGreen = green;
  }

  /**
   * @param blue the border blue color.
   */
  public void setBorderColorBlue( int blue ) {
    this.borderColorBlue = blue;
  }

  /**
   * @parm red the backGround red color.
   */
  public void setBackGroundColorRed( int red ) {
    this.backgroundColorRed = red;
  }

  /**
   * @parm green the backGround green color.
   */
  public void setBackGroundColorGreen( int green ) {
    this.backgroundColorGreen = green;
  }

  /**
   * @parm green the backGround blue color.
   */
  public void setBackGroundColorBlue( int blue ) {
    this.backgroundColorBlue = blue;
  }

  /**
   * @returns the font color red.
   */
  public void setFontColorRed( int red ) {
    this.fontColorRed = red;
  }

  /**
   * @param green the font color green.
   */
  public void setFontColorGreen( int green ) {
    this.fontColorGreen = green;
  }

  /**
   * @param blue the font color blue.
   */
  public void setFontColorBlue( int blue ) {
    this.fontColorBlue = blue;
  }

  /**
   * @return Returns the selected.
   */
  public boolean isSelected() {
    return selected;
  }

  /**
   * @param selected The selected to set.
   */
  public void setSelected( boolean selected ) {
    this.selected = selected;
  }

  /**
   * Change a selected state to not-selected and vice-versa.
   */
  public void flipSelected() {
    this.selected = !this.selected;
  }

  public NotePadMeta clone() {
    return new NotePadMeta( this );
  }

  public void setChanged() {
    setChanged( true );
  }

  public void setChanged( boolean ch ) {
    changed = ch;
  }

  public boolean hasChanged() {
    return changed;
  }

  public String toString() {
    return note;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( "    <notepad>" ).append( Const.CR );
    retval.append( "      " ).append( XmlHandler.addTagValue( "note", note ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "xloc", location.x ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "yloc", location.y ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "width", width ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "heigth", height ) );
    // Font
    retval.append( "      " ).append( XmlHandler.addTagValue( "fontname", fontName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "fontsize", fontSize ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "fontbold", fontBold ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "fontitalic", fontItalic ) );
    // Font color
    retval.append( "      " ).append( XmlHandler.addTagValue( "fontcolorred", fontColorRed ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "fontcolorgreen", fontColorGreen ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "fontcolorblue", fontColorBlue ) );
    // Background color
    retval.append( "      " ).append( XmlHandler.addTagValue( "backgroundcolorred", backgroundColorRed ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "backgroundcolorgreen", backgroundColorGreen ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "backgroundcolorblue", backgroundColorBlue ) );
    // border color
    retval.append( "      " ).append( XmlHandler.addTagValue( "bordercolorred", borderColorRed ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "bordercolorgreen", borderColorGreen ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "bordercolorblue", borderColorBlue ) );
    retval.append( "    </notepad>" ).append( Const.CR );

    return retval.toString();
  }

  /**
   * @return the height
   */
  public int getHeight() {
    return height;
  }

  /**
   * @param height the height to set
   */
  public void setHeight( int height ) {
    this.height = height;
  }

  /**
   * @return the width
   */
  public int getWidth() {
    return width;
  }

  /**
   * @param width the width to set
   */
  public void setWidth( int width ) {
    this.width = width;
  }

  /**
   * @return Returns the font name.
   */
  public String getFontName() {
    return this.fontName;
  }

  /**
   * @param fontname The font name.
   */
  public void setFontName( String fontname ) {
    this.fontName = fontname;
  }

  /**
   * @return Returns the font size.
   */
  public int getFontSize() {
    return this.fontSize;
  }

  /**
   * @param fontbold The font bold.
   */
  public void setFontBold( boolean fontbold ) {
    this.fontBold = fontbold;
  }

  /**
   * @return Returns the font bold.
   */
  public boolean isFontBold() {
    return this.fontBold;
  }

  /**
   * @param fontitalic The font italic.
   */
  public void setFontItalic( boolean fontitalic ) {
    this.fontItalic = fontitalic;
  }

  /**
   * @return Returns the font italic.
   */
  public boolean isFontItalic() {
    return this.fontItalic;
  }

  /**
   * @return Returns the backGround color red.
   */
  public int getBorderColorRed() {
    return this.borderColorRed;
  }

  /**
   * @return Returns the backGround color green.
   */
  public int getBorderColorGreen() {
    return this.borderColorGreen;
  }

  /**
   * @return Returns the backGround color blue.
   */
  public int getBorderColorBlue() {
    return this.borderColorBlue;
  }

  /**
   * @return Returns the backGround color red.
   */
  public int getBackGroundColorRed() {
    return this.backgroundColorRed;
  }

  /**
   * @return Returns the backGround color green.
   */
  public int getBackGroundColorGreen() {
    return this.backgroundColorGreen;
  }

  /**
   * @return Returns the backGround color blue.
   */
  public int getBackGroundColorBlue() {
    return this.backgroundColorBlue;
  }

  /**
   * @return Returns the font color red.
   */
  public int getFontColorRed() {
    return this.fontColorRed;
  }

  /**
   * @return Returns the font color green.
   */
  public int getFontColorGreen() {
    return this.fontColorGreen;
  }

  /**
   * @return Returns the font color blue.
   */
  public int getFontColorBlue() {
    return this.fontColorBlue;
  }

  /**
   * @param fontsize The font name.
   */
  public void setFontSize( int fontsize ) {
    this.fontSize = fontsize;
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
    this.backgroundColorRed = COLOR_RGB_DEFAULT_BG_RED;
    this.backgroundColorGreen = COLOR_RGB_DEFAULT_BG_GREEN;
    this.backgroundColorBlue = COLOR_RGB_DEFAULT_BG_BLUE;

    // border gray
    this.borderColorRed = COLOR_RGB_DEFAULT_BORDER_RED;
    this.borderColorGreen = COLOR_RGB_DEFAULT_BORDER_GREEN;
    this.borderColorBlue = COLOR_RGB_DEFAULT_BORDER_BLUE;
  }
}
