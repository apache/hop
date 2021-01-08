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

package org.apache.hop.ui.hopgui.shared;

import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.SwtUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.LineAttributes;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.graphics.Transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SwtGc implements IGc {

  protected Color background;

  protected Color black;
  protected Color white;
  protected Color red;
  protected Color yellow;
  protected Color hopFalse;
  protected Color green;
  protected Color blue;
  protected Color magenta;
  protected Color purpule;
  protected Color indigo;
  protected Color gray;
  protected Color lightGray;
  protected Color darkGray;
  protected Color lightBlue;
  protected Color crystal;
  protected Color hopDefault;
  protected Color hopTrue;
  protected Color deprecated;

  private GC gc;

  private int iconSize;

  private int miniIconSize;

  private Map<String, SwtUniversalImage> images;

  private float currentMagnification = 1.0f;

  private List<Color> colors;
  private List<Font> fonts;

  private Point area;
  private Transform transform;

  public SwtGc( GC gc, int width, int height, int iconSize ) {
    this.gc = gc;
    this.images = GuiResource.getInstance().getImagesTransforms();
    this.iconSize = iconSize;
    this.miniIconSize = iconSize / 2;
    this.area = new Point(width, height);

    this.colors = new ArrayList<>();
    this.fonts = new ArrayList<>();

    this.background = GuiResource.getInstance().getColorGraph();
    this.black = GuiResource.getInstance().getColorBlack();
    this.white = GuiResource.getInstance().getColorWhite();
    this.red = GuiResource.getInstance().getColorRed();
    this.yellow = GuiResource.getInstance().getColorYellow();
    this.hopFalse = GuiResource.getInstance().getColorOrange();
    this.green = GuiResource.getInstance().getColorGreen();
    this.blue = GuiResource.getInstance().getColorBlue();
    this.magenta = GuiResource.getInstance().getColorMagenta();
    this.purpule = GuiResource.getInstance().getColorPurpule();
    this.indigo = GuiResource.getInstance().getColorIndigo();
    this.gray = GuiResource.getInstance().getColorGray();
    this.lightGray = GuiResource.getInstance().getColorLightGray();
    this.darkGray = GuiResource.getInstance().getColorDarkGray();
    this.lightBlue = GuiResource.getInstance().getColorLightBlue();
    this.crystal = GuiResource.getInstance().getColorCrystalText();
    this.hopDefault = GuiResource.getInstance().getColorHopDefault();
    this.hopTrue = GuiResource.getInstance().getColorHopTrue();
    this.deprecated = GuiResource.getInstance().getColorDeprecated();
  }

  public void dispose() {
    // Do not dispose the GC.  It's handed to us so we don't dispose it
    // However, the resources below are possibly used and allocated here so they need to be cleaned up
    //
    if ( transform != null && transform.isDisposed() == false ) {
      transform.dispose();
    }
    for ( Color color : colors ) {
      color.dispose();
    }
    for ( Font font : fonts ) {
      font.dispose();
    }
  }

  public void drawLine( int x, int y, int x2, int y2 ) {
    gc.drawLine( x, y, x2, y2 );
  }

  public void drawImage( EImage image, int x, int y, float magnification ) {
    Image img = getNativeImage( image ).getAsBitmapForSize( gc.getDevice(), Math.round( miniIconSize * magnification ),
      Math.round( miniIconSize * magnification ) );
    if ( img != null ) {
      Rectangle bounds = img.getBounds();
      gc.drawImage( img, 0, 0, bounds.width, bounds.height, x, y, miniIconSize, miniIconSize );
    }
  }

  public void drawImage( EImage image, int x, int y, int width, int height, float magnification ) {
    Image img = getNativeImage( image ).getAsBitmapForSize( gc.getDevice(), Math.round( width * magnification ),
      Math.round( height * magnification ) );
    if ( img != null ) {
      Rectangle bounds = img.getBounds();
      gc.drawImage( img, 0, 0, bounds.width, bounds.height, x, y, width, height );
    }
  }

  public void drawImage( EImage image, int x, int y, float magnification, double angle ) {
    Image img =
      getNativeImage( image ).getAsBitmapForSize( gc.getDevice(), Math.round( miniIconSize * magnification ),
        Math.round( miniIconSize * magnification ), angle );
    if ( img != null ) {
      Rectangle bounds = img.getBounds();
      int hx = Math.round( bounds.width / magnification );
      int hy = Math.round( bounds.height / magnification );
      gc.drawImage( img, 0, 0, bounds.width, bounds.height, x - hx / 2, y - hy / 2, hx, hy );
    }
  }

  public static final SwtUniversalImage getNativeImage( EImage image ) {
    switch ( image ) {
      case LOCK:
        return GuiResource.getInstance().getSwtImageLocked();
      case FAILURE:
        return GuiResource.getInstance().getSwtImageFailure();
      case EDIT:
        return GuiResource.getInstance().getSwtImageEdit();
      case CONTEXT_MENU:
        return GuiResource.getInstance().getSwtImageContextMenu();
      case TRUE:
        return GuiResource.getInstance().getSwtImageTrue();
      case FALSE:
        return GuiResource.getInstance().getSwtImageFalse();
      case ERROR:
        return GuiResource.getInstance().getSwtImageError();
      case SUCCESS:
        return GuiResource.getInstance().getSwtImageSuccess();
      case INFO:
        return GuiResource.getInstance().getSwtImageInfo();
      case TARGET:
        return GuiResource.getInstance().getSwtImageTarget();
      case INPUT:
        return GuiResource.getInstance().getSwtImageInput();
      case OUTPUT:
        return GuiResource.getInstance().getSwtImageOutput();
      case ARROW:
        return GuiResource.getInstance().getSwtImageArrow();
      case COPY_ROWS:
        return GuiResource.getInstance().getSwtImageCopyRows();
      case LOAD_BALANCE:
        return GuiResource.getInstance().getSwtImageBalance();
      case CHECKPOINT:
        return GuiResource.getInstance().getSwtImageCheckpoint();
      case DB:
        return GuiResource.getInstance().getSwtImageConnection();
      case PARALLEL:
        return GuiResource.getInstance().getSwtImageParallelHop();
      case UNCONDITIONAL:
        return GuiResource.getInstance().getSwtImageUnconditionalHop();
      case BUSY:
        return GuiResource.getInstance().getSwtImageBusy();
      case INJECT:
        return GuiResource.getInstance().getSwtImageInject();
      case ARROW_DEFAULT:
        return GuiResource.getInstance().getSwtImageArrowDefault();
      case ARROW_TRUE:
        return GuiResource.getInstance().getSwtImageArrowTrue();
      case ARROW_FALSE:
        return GuiResource.getInstance().getSwtImageArrowFalse();
      case ARROW_ERROR:
        return GuiResource.getInstance().getSwtImageArrowError();
      case ARROW_DISABLED:
        return GuiResource.getInstance().getSwtImageArrowDisabled();
      case ARROW_CANDIDATE:
        return GuiResource.getInstance().getSwtImageArrowCandidate();
      case DATA:
        return GuiResource.getInstance().getSwtImageData();
      default:
        break;
    }
    return null;
  }

  public void drawPoint( int x, int y ) {
    gc.drawPoint( x, y );
  }

  public void drawPolygon( int[] polygon ) {
    gc.drawPolygon( polygon );
  }

  public void drawPolyline( int[] polyline ) {
    gc.drawPolyline( polyline );
  }

  public void drawRectangle( int x, int y, int width, int height ) {
    gc.drawRectangle( x, y, width, height );
  }

  public void drawRoundRectangle( int x, int y, int width, int height, int circleWidth, int circleHeight ) {
    gc.drawRoundRectangle( x, y, width, height, circleWidth, circleHeight );
  }

  public void drawText( String text, int x, int y ) {
    gc.drawText( text, x, y );
  }

  public void drawText( String text, int x, int y, boolean transparent ) {
    gc.drawText( text, x, y, SWT.DRAW_DELIMITER | SWT.DRAW_TAB | SWT.DRAW_TRANSPARENT );
  }

  public void fillPolygon( int[] polygon ) {
    gc.fillPolygon( polygon );
  }

  public void fillRectangle( int x, int y, int width, int height ) {
    gc.fillRectangle( x, y, width, height );
  }

  public void fillGradientRectangle( int x, int y, int width, int height, boolean vertical ) {
    gc.fillGradientRectangle( x, y, width, height, vertical );
  }

  public void fillRoundRectangle( int x, int y, int width, int height, int circleWidth, int circleHeight ) {
    gc.fillRoundRectangle( x, y, width, height, circleWidth, circleHeight );
  }

  public Point getDeviceBounds() {
    org.eclipse.swt.graphics.Rectangle p = gc.getDevice().getBounds();
    return new Point( p.width, p.height );
  }

  public void setAlpha( int alpha ) {
    gc.setAlpha( alpha );
  }

  public int getAlpha() {
    return gc.getAlpha();
  }

  public void setBackground( EColor color ) {
    gc.setBackground( getColor( color ) );
  }

  private Color getColor( EColor color ) {
    switch ( color ) {
      case BACKGROUND:
        return background;
      case BLACK:
        return black;
      case WHITE:
        return white;
      case RED:
        return red;
      case YELLOW:
        return yellow;
      case GREEN:
        return green;
      case BLUE:
        return blue;
      case MAGENTA:
        return magenta;
      case PURPULE:
        return purpule;
      case INDIGO:
        return indigo;        
      case GRAY:
        return gray;
      case LIGHTGRAY:
        return lightGray;
      case DARKGRAY:
        return darkGray;
      case LIGHTBLUE:
        return lightBlue;
      case CRYSTAL:
        return crystal;
      case HOP_DEFAULT:
        return hopDefault;
      case HOP_TRUE:
        return hopTrue;
      case HOP_FALSE:
        return hopFalse;        
      case DEPRECATED:
        return deprecated;
      default:
        break;
    }
    return null;
  }

  public void setFont( EFont font ) {
    switch ( font ) {
      case GRAPH:
        gc.setFont( GuiResource.getInstance().getFontGraph() );
        break;
      case NOTE:
        gc.setFont( GuiResource.getInstance().getFontNote() );
        break;
      case SMALL:
        gc.setFont( GuiResource.getInstance().getFontSmall() );
        break;
      default:
        break;
    }
  }

  public void setForeground( EColor color ) {
    gc.setForeground( getColor( color ) );
  }

  public void setLineStyle( ELineStyle lineStyle ) {
    // RAP does not implement LineStyle and LineAttributes
    if (!EnvironmentUtils.getInstance().isWeb()) {
      switch (lineStyle) {
        case DASHDOT:
          gc.setLineStyle(SWT.LINE_DASHDOT);
          break;
        case SOLID:
          gc.setLineStyle(SWT.LINE_SOLID);
          break;
        case DOT:
          gc.setLineStyle(SWT.LINE_DOT);
          break;
        case DASH:
          gc.setLineStyle(SWT.LINE_DASH);
          break;
        case PARALLEL:
          gc.setLineAttributes(
              new LineAttributes(
                  gc.getLineWidth(),
                  SWT.CAP_FLAT,
                  SWT.JOIN_MITER,
                  SWT.LINE_CUSTOM,
                  new float[] {
                    5, 3,
                  },
                  0,
                  10));
          break;
        default:
          break;
      }
    }
  }

  public void setLineWidth( int width ) {
    gc.setLineWidth( width );
  }

  public void setTransform( float translationX, float translationY, float magnification ) {
    if ( transform != null ) { // dispose of previous to prevent leaking of handles
      transform.dispose();
    }
    transform = new Transform( gc.getDevice() );
    transform.translate( translationX, translationY );
    transform.scale( magnification, magnification );
    gc.setTransform( transform );
    currentMagnification = magnification;
  }

  @Override public float getMagnification() {
    return currentMagnification;
  }

  public Point textExtent( String text ) {
    org.eclipse.swt.graphics.Point p = gc.textExtent( text );
    return new Point( p.x, p.y );
  }

  public void drawTransformIcon( int x, int y, TransformMeta transformMeta, float magnification ) {
    SwtUniversalImage swtImage = null;
    
    if ( transformMeta.isMissing() ) {
      swtImage = GuiResource.getInstance().getSwtImageMissing();
    } else if ( transformMeta.isDeprecated() ) {
      swtImage = GuiResource.getInstance().getSwtImageDeprecated();
    } else {
      String pluginId = transformMeta.getPluginId();
      if ( pluginId != null ) {
        swtImage = GuiResource.getInstance().getImagesTransforms().get( pluginId );
      }
    }

    if ( swtImage == null ) {
      return;
    }

    int w = Math.round( iconSize * magnification );
    int h = Math.round( iconSize * magnification );
    Image image = swtImage.getAsBitmapForSize( gc.getDevice(), w, h );

    org.eclipse.swt.graphics.Rectangle bounds = image.getBounds();
    gc.drawImage( image, 0, 0, bounds.width, bounds.height, x, y, iconSize, iconSize );
  }

  public void drawActionIcon( int x, int y, ActionMeta actionMeta, float magnification ) {
    if ( actionMeta == null ) {
      return; // Don't draw anything
    }

    SwtUniversalImage swtImage = null;

    int w = Math.round( iconSize * magnification );
    int h = Math.round( iconSize * magnification );

    if ( actionMeta.isMissing() ) {
	  swtImage = GuiResource.getInstance().getSwtImageMissing();
    }
    else if ( actionMeta.isDeprecated() ) {
      swtImage = GuiResource.getInstance().getSwtImageDeprecated();
    }
    else {
      String pluginId = actionMeta.getAction().getPluginId();
      if ( pluginId != null ) {
        swtImage = GuiResource.getInstance().getImagesActions().get( pluginId );
      }
    }

    if ( swtImage == null ) {
      return;
    }

    Image image = swtImage.getAsBitmapForSize( gc.getDevice(), w, h );

    org.eclipse.swt.graphics.Rectangle bounds = image.getBounds();
    gc.drawImage( image, 0, 0, bounds.width, bounds.height, x, y, iconSize, iconSize );
  }

  @Override public void drawImage( SvgFile svgFile, int x, int y, int desiredWidth, int desiredHeight, float magnification, double angle ) throws HopException {
    //
    SvgCacheEntry cacheEntry = SvgCache.loadSvg( svgFile );
    SwtUniversalImageSvg imageSvg = new SwtUniversalImageSvg( new SvgImage(cacheEntry.getSvgDocument()) );

    int magnifiedWidth = Math.round( desiredWidth * magnification );
    int magnifiedHeight = Math.round( desiredHeight * magnification );
    if (angle!=0) {
      // A rotated image is blown up to twice its size to allow it to be rendered completely in the center
      //
      Image img = imageSvg.getAsBitmapForSize( gc.getDevice(), magnifiedWidth, magnifiedHeight, angle );
      Rectangle bounds = img.getBounds();
      int hx = Math.round( bounds.width / magnification );
      int hy = Math.round( bounds.height / magnification );
      gc.drawImage( img, 0, 0, bounds.width, bounds.height, x - hx / 2, y - hy / 2, hx, hy );
    } else {
      // Without rotation we simply draw the image with the desired width
      //
      Image img = imageSvg.getAsBitmapForSize( gc.getDevice(), magnifiedWidth, magnifiedHeight );
      Rectangle bounds = img.getBounds();
      gc.drawImage( img, 0, 0, bounds.width, bounds.height, x, y, desiredWidth, desiredHeight );
    }
  }

  public void setAntialias( boolean antiAlias ) {
    if ( antiAlias ) {
      gc.setAntialias( SWT.ON );
    } else {
      gc.setAntialias( SWT.OFF );
    }
  }

  public void setBackground( int r, int g, int b ) {
    Color color = getColor( r, g, b );
    gc.setBackground( color );
  }

  public void setForeground( int r, int g, int b ) {
    Color color = getColor( r, g, b );
    gc.setForeground( color );
  }

  private Color getColor( int r, int g, int b ) {
    Color color = new Color( PropsUi.getDisplay(), new RGB( r, g, b ) );
    int index = colors.indexOf( color );
    if ( index < 0 ) {
      colors.add( color );
    } else {
      color.dispose();
      color = colors.get( index );
    }
    return color;
  }

  public void setFont( String fontName, int fontSize, boolean fontBold, boolean fontItalic ) {
    int swt = SWT.NORMAL;
    if ( fontBold ) {
      swt = SWT.BOLD;
    }
    if ( fontItalic ) {
      swt = swt | SWT.ITALIC;
    }

    Font font = new Font( PropsUi.getDisplay(), fontName, fontSize, swt );
    int index = fonts.indexOf( font );
    if ( index < 0 ) {
      fonts.add( font );
    } else {
      font.dispose();
      font = fonts.get( index );
    }
    gc.setFont( font );
  }

  public void switchForegroundBackgroundColors() {
    Color fg = gc.getForeground();
    Color bg = gc.getBackground();

    gc.setForeground( bg );
    gc.setBackground( fg );
  }

  public Point getArea() {
    return area;
  }

}
