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

package org.apache.hop.core.gui;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

public interface IGc {


  enum EColor {
    BACKGROUND, BLACK, WHITE, RED, YELLOW, HOP_FALSE, GREEN, BLUE, MAGENTA, PURPULE, INDIGO, GRAY, LIGHTGRAY, DARKGRAY, LIGHTBLUE, CRYSTAL,
    HOP_DEFAULT, HOP_TRUE, DEPRECATED
  }

  enum EFont {
    NOTE, GRAPH, SMALL,
  }

  enum ELineStyle {
    SOLID, DASHDOT, DOT, PARALLEL, DASH
  }

  enum EImage {
    LOCK, EDIT, CONTEXT_MENU, TRUE, FALSE, ERROR, INFO, SUCCESS, FAILURE, TARGET, INPUT, OUTPUT, ARROW,
    COPY_ROWS, UNCONDITIONAL, PARALLEL, BUSY, INJECT, LOAD_BALANCE, CHECKPOINT, DB, ARROW_DEFAULT, ARROW_TRUE, ARROW_FALSE,
    ARROW_ERROR, ARROW_DISABLED, ARROW_CANDIDATE, DATA
    ;
  }

  void setLineWidth( int width );

  void setFont( EFont font );

  Point textExtent( String text );

  Point getDeviceBounds();

  void setBackground( EColor color );

  void setForeground( EColor color );

  void setBackground( int red, int green, int blue );

  void setForeground( int red, int green, int blue );

  void fillRectangle( int x, int y, int width, int height );

  void fillGradientRectangle( int x, int y, int width, int height, boolean vertical );


  void drawImage( EImage image, int x, int y, float magnification ) throws HopException;

  void drawImage( EImage image, int x, int y, float magnification, double angle ) throws HopException;

  void drawImage( SvgFile svgFile, int x, int y, int desiredWidth, int desiredHeight, float magnification, double angle ) throws HopException;


  void drawLine( int x, int y, int x2, int y2 );

  void setLineStyle( ELineStyle lineStyle );

  void drawRectangle( int x, int y, int width, int height );

  void drawPoint( int x, int y );

  void drawText( String text, int x, int y );

  void drawText( String text, int x, int y, boolean transparent );

  void fillRoundRectangle( int x, int y, int width, int height, int circleWidth, int circleHeight );

  void drawRoundRectangle( int x, int y, int width, int height, int circleWidth, int circleHeight );

  void fillPolygon( int[] polygon );

  void drawPolygon( int[] polygon );

  void drawPolyline( int[] polyline );

  void setAntialias( boolean antiAlias );

  void setTransform( float translationX, float translationY, float magnification );

  float getMagnification();

  void setAlpha( int alpha );

  void dispose();

  int getAlpha();

  void setFont( String fontName, int fontSize, boolean fontBold, boolean fontItalic );

  void switchForegroundBackgroundColors();

  Point getArea();

  void drawTransformIcon( int x, int y, TransformMeta transformMeta, float magnification ) throws HopException;

  void drawActionIcon( int x, int y, ActionMeta actionMeta, float magnification ) throws HopException;
}
