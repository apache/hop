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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.svg.SVGDocument;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.util.HashMap;
import java.util.Map;


public class SvgGc implements IGc {

  private static SvgFile imageLocked;

  private static SvgFile imageFailure;

  private static SvgFile imageEdit;

  private static SvgFile imageContextMenu;

  private static SvgFile imageTrue;

  private static SvgFile imageFalse;

  private static SvgFile imageError;

  private static SvgFile imageInfo;

  private static SvgFile imageTarget;

  private static SvgFile imageInput;

  private static SvgFile imageOutput;

  private static SvgFile imageArrow;

  private static SvgFile imageCopyRows;

  private static SvgFile imageLoadBalance;

  private static SvgFile imageCheckpoint;

  private static SvgFile imageDatabase;

  private static SvgFile imageParallel;

  private static SvgFile imageUnconditional;

  private static SvgFile imageBusy;

  private static SvgFile imageMissing;
  
  private static SvgFile imageDeprecated;

  private static SvgFile imageInject;

  private static SvgFile imageData;

  private static SvgFile imageArrowDefault;
  private static SvgFile imageArrowTrue;
  private static SvgFile imageArrowFalse;
  private static SvgFile imageArrowError;
  private static SvgFile imageArrowDisabled;

  protected Color background;

  protected Color black;
  protected Color red;
  protected Color yellow;
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
  protected Color hopFalse;
  protected Color deprecated;

  private HopSvgGraphics2D gc;

  private int iconSize;
  private int miniIconSize;

  private Map<String, SvgFile> transformImages;
  private Map<String, SvgFile> actionImages;

  private Point area;

  private int alpha;

  private Font fontGraph;

  private Font fontNote;

  private Font fontSmall;

  private int lineWidth;
  private ELineStyle lineStyle;

  private int yOffset;

  private int xOffset;

  private AffineTransform originalTransform;

  public SvgGc( HopSvgGraphics2D gc, Point area, int iconSize, int xOffset, int yOffset ) throws HopException {
    this.gc = gc;
    this.transformImages = getTransformImageFilenames();
    this.actionImages = getActionImageFilenames();
    this.iconSize = iconSize;
    this.miniIconSize = iconSize / 2;
    this.area = area;
    this.xOffset = xOffset;
    this.yOffset = yOffset;
    this.originalTransform = this.gc.getTransform();

    gc.setSVGCanvasSize( new Dimension(area.x, area.y) );

    init();
  }

  private Map<String, SvgFile> getTransformImageFilenames() throws HopPluginException {
    Map<String, SvgFile> map = new HashMap<>();
    PluginRegistry registry = PluginRegistry.getInstance();
    for ( IPlugin plugin : registry.getPlugins( TransformPluginType.class ) ) {
      for ( String id : plugin.getIds() ) {
        map.put( id, new SvgFile( plugin.getImageFile(), registry.getClassLoader( plugin ) ) );
      }
    }
    return map;
  }

  private Map<String, SvgFile> getActionImageFilenames() throws HopPluginException {
    Map<String, SvgFile> map = new HashMap<>();
    PluginRegistry registry = PluginRegistry.getInstance();

    for ( IPlugin plugin : registry.getPlugins( ActionPluginType.class ) ) {
      for ( String id : plugin.getIds() ) {
        map.put( id, new SvgFile( plugin.getImageFile(), registry.getClassLoader( plugin ) ) );
      }
    }
    return map;
  }


  private void init() throws HopException {
    this.lineStyle = ELineStyle.SOLID;
    this.lineWidth = 1;
    this.alpha = 255;

    this.background = new Color( 255, 255, 255 );
    this.black = new Color( 0, 0, 0 );
    this.red = new Color( 255, 0, 0 );
    this.yellow = new Color( 255, 255, 0 );
    this.green = new Color( 0, 255, 0 );
    this.blue = new Color( 0, 0, 255 );
    this.magenta = new Color( 255, 0, 255 );
    this.purpule = new Color( 128, 0, 128 );
    this.indigo = new Color( 75, 0, 130 );
    this.gray = new Color( 215, 215, 215 );    
    this.lightGray = new Color( 225, 225, 225 );
    this.darkGray = new Color( 100, 100, 100 );
    this.lightBlue = new Color( 135, 206, 250 ); // light sky blue
    this.crystal = new Color( 61, 99, 128 );
    this.hopDefault = new Color( 61, 99, 128 );
    this.hopTrue = new Color( 12, 178, 15 );
    this.hopFalse = new Color( 255, 165, 0 );
    this.deprecated = new Color( 246, 196, 56 );
    
    imageLocked = new SvgFile("ui/images/lock.svg", this.getClass().getClassLoader() );
    imageFailure = new SvgFile("ui/images/failure.svg", this.getClass().getClassLoader() );
    imageEdit = new SvgFile("ui/images/edit.svg", this.getClass().getClassLoader() );
    imageContextMenu = new SvgFile("ui/images/settings.svg", this.getClass().getClassLoader() ); // Used ?
    imageTrue = new SvgFile("ui/images/true.svg", this.getClass().getClassLoader() );
    imageFalse = new SvgFile("ui/images/false.svg", this.getClass().getClassLoader() );
    imageError = new SvgFile( "ui/images/error.svg", this.getClass().getClassLoader() );
    imageInfo = new SvgFile("ui/images/info.svg", this.getClass().getClassLoader() );
    imageTarget = new SvgFile("ui/images/target.svg", this.getClass().getClassLoader() );
    imageInput = new SvgFile("ui/images/input.svg", this.getClass().getClassLoader() );
    imageOutput = new SvgFile("ui/images/output.svg", this.getClass().getClassLoader() );
    imageArrow = new SvgFile("ui/images/arrow.svg", this.getClass().getClassLoader() );
    imageCopyRows = new SvgFile("ui/images/copy-rows.svg", this.getClass().getClassLoader() );
    imageLoadBalance = new SvgFile("ui/images/scales.svg", this.getClass().getClassLoader() );
    imageCheckpoint = new SvgFile("ui/images/checkpoint.svg" , this.getClass().getClassLoader() );
    imageDatabase = new SvgFile("ui/images/database.svg", this.getClass().getClassLoader() );
    imageParallel = new SvgFile("ui/images/parallel-hop.svg", this.getClass().getClassLoader() );
    imageUnconditional = new SvgFile("ui/images/unconditional-hop.svg", this.getClass().getClassLoader() );
    imageBusy = new SvgFile("ui/images/busy.svg", this.getClass().getClassLoader() );
    imageInject = new SvgFile("ui/images/inject.svg", this.getClass().getClassLoader() );
    imageMissing = new SvgFile("ui/images/missing.svg", this.getClass().getClassLoader() );
    imageDeprecated = new SvgFile("ui/images/deprecated.svg", this.getClass().getClassLoader() );
       
    // Hop arrow
    //
    imageArrowDefault = new SvgFile("ui/images/hop-arrow-default.svg", this.getClass().getClassLoader() );
    imageArrowFalse = new SvgFile("ui/images/hop-arrow-false.svg", this.getClass().getClassLoader() );
    imageArrowTrue = new SvgFile("ui/images/hop-arrow-true.svg", this.getClass().getClassLoader() );
    imageArrowError = new SvgFile("ui/images/hop-arrow-error.svg", this.getClass().getClassLoader() );
    imageArrowDisabled = new SvgFile("ui/images/hop-arrow-disabled.svg", this.getClass().getClassLoader() );
   
    
    fontGraph = new Font( "FreeSans", Font.PLAIN, 10 );
    fontNote = new Font( "FreeSans", Font.PLAIN, 10 );
    fontSmall = new Font( "FreeSans", Font.PLAIN, 8 );

    gc.setFont( fontGraph );

    gc.setColor( background );
    gc.fillRect( 0, 0, area.x, area.y );
  }

  public void dispose() {
  }

  public void drawLine( int x, int y, int x2, int y2 ) {
    gc.drawLine( x + xOffset, y + yOffset, x2 + xOffset, y2 + yOffset );
  }

  public void drawPoint( int x, int y ) {
    gc.drawLine( x + xOffset, y + yOffset, x + xOffset, y + yOffset );
  }

  public void drawPolygon( int[] polygon ) {
    gc.drawPolygon( getSwingPolygon( polygon ) );
  }

  private Polygon getSwingPolygon( int[] polygon ) {
    int nPoints = polygon.length / 2;
    int[] xPoints = new int[ polygon.length / 2 ];
    int[] yPoints = new int[ polygon.length / 2 ];
    for ( int i = 0; i < nPoints; i++ ) {
      xPoints[ i ] = polygon[ 2 * i + 0 ] + xOffset;
      yPoints[ i ] = polygon[ 2 * i + 1 ] + yOffset;
    }

    return new Polygon( xPoints, yPoints, nPoints );
  }

  public void drawPolyline( int[] polyline ) {
    int nPoints = polyline.length / 2;
    int[] xPoints = new int[ polyline.length / 2 ];
    int[] yPoints = new int[ polyline.length / 2 ];
    for ( int i = 0; i < nPoints; i++ ) {
      xPoints[ i ] = polyline[ 2 * i + 0 ] + xOffset;
      yPoints[ i ] = polyline[ 2 * i + 1 ] + yOffset;
    }
    gc.drawPolyline( xPoints, yPoints, nPoints );
  }

  public void drawRectangle( int x, int y, int width, int height ) {
    gc.drawRect( x + xOffset, y + yOffset, width, height );
  }

  public void drawRoundRectangle( int x, int y, int width, int height, int circleWidth, int circleHeight ) {
    gc.drawRoundRect( x + xOffset, y + yOffset, width, height, circleWidth, circleHeight );
  }

  public void drawText( String text, int x, int y ) {

    int height = gc.getFontMetrics().getHeight();
    int descent = gc.getFontMetrics().getDescent();

    String[] lines = text.split( "\n" );
    for ( String line : lines ) {
      gc.drawString( line, x + xOffset, y + height + yOffset - descent );
      y += height;
    }
  }

  public void drawText( String text, int x, int y, boolean transparent ) {
    drawText( text, x, y );
  }

  public void fillPolygon( int[] polygon ) {
    switchForegroundBackgroundColors();
    gc.fillPolygon( getSwingPolygon( polygon ) );
    switchForegroundBackgroundColors();
  }

  public void fillRectangle( int x, int y, int width, int height ) {
    switchForegroundBackgroundColors();
    gc.fillRect( x + xOffset, y + yOffset, width, height );
    switchForegroundBackgroundColors();
  }

  // TODO: complete code
  public void fillGradientRectangle( int x, int y, int width, int height, boolean vertical ) {
    fillRectangle( x, y, width, height );
  }

  public void fillRoundRectangle( int x, int y, int width, int height, int circleWidth, int circleHeight ) {
    switchForegroundBackgroundColors();
    gc.fillRoundRect( x + xOffset, y + yOffset, width, height, circleWidth, circleHeight );
    switchForegroundBackgroundColors();
  }

  public Point getDeviceBounds() {
    return area;
  }

  public void setAlpha( int alpha ) {
    this.alpha = alpha;
    AlphaComposite alphaComposite = AlphaComposite.getInstance( AlphaComposite.SRC_OVER, alpha / 255 );
    gc.setComposite( alphaComposite );
  }

  public int getAlpha() {
    return alpha;
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
        gc.setFont( fontGraph );
        break;
      case NOTE:
        gc.setFont( fontNote );
        break;
      case SMALL:
        gc.setFont( fontSmall );
        break;
      default:
        break;
    }
  }

  public void setForeground( EColor color ) {
    gc.setColor( getColor( color ) );
  }

  public void setLineStyle( ELineStyle lineStyle ) {
    this.lineStyle = lineStyle;
    gc.setStroke( createStroke() );
  }

  private Stroke createStroke() {
    float[] dash;
    switch ( lineStyle ) {
      case SOLID:
        dash = null;
        break;
      case DOT:
        dash = new float[] { 5, };
        break;
      case DASHDOT:
        dash = new float[] { 10, 5, 5, 5, };
        break;
      case PARALLEL:
        dash = new float[] { 10, 5, 10, 5, };
        break;
      case DASH:
        dash = new float[] { 6, 2, };
        break;
      default:
        throw new RuntimeException( "Unhandled line style!" );
    }
    return new BasicStroke( lineWidth, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 2, dash, 0 );
  }

  public void setLineWidth( int width ) {
    this.lineWidth = width;
    gc.setStroke( createStroke() );
  }

  public void setTransform( float translationX, float translationY, float magnification ) {
    // PDI-9953 - always use original GC's transform.
    AffineTransform transform = (AffineTransform) originalTransform.clone();
    transform.translate( translationX, translationY );
    transform.scale( magnification, magnification );
    gc.setTransform( transform );
  }

  @Override public float getMagnification() {
    return (float) gc.getTransform().getScaleX();
  }

  public AffineTransform getTransform() {
    return gc.getTransform();
  }

  public Point textExtent( String text ) {

    String[] lines = text.split( Const.CR );
    int maxWidth = 0;
    for ( String line : lines ) {
      Rectangle2D bounds = gc.getFontMetrics().getStringBounds( line, gc );
      if ( bounds.getWidth() > maxWidth ) {
        maxWidth = (int) bounds.getWidth();
      }
    }
    int height = gc.getFontMetrics().getHeight() * lines.length;

    return new Point( maxWidth, height );
  }


  public void setAntialias( boolean antiAlias ) {
    if ( antiAlias ) {
      RenderingHints hints = new RenderingHints( RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON );
      hints.add( new RenderingHints( RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY ) );
      hints.add( new RenderingHints( RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON ) );
      gc.setRenderingHints( hints );
    }
  }

  public void setBackground( int r, int g, int b ) {
    Color color = getColor( r, g, b );
    gc.setBackground( color );
  }

  public void setForeground( int r, int g, int b ) {
    Color color = getColor( r, g, b );
    gc.setColor( color );
  }

  private Color getColor( int r, int g, int b ) {
    return new Color( r, g, b );
  }

  public void setFont( String fontName, int fontSize, boolean fontBold, boolean fontItalic ) {
    int style = Font.PLAIN;
    if ( fontBold ) {
      style = Font.BOLD;
    }
    if ( fontItalic ) {
      style = style | Font.ITALIC;
    }

    Font font = new Font( fontName, style, fontSize );
    gc.setFont( font );
  }

  public Object getImage() {
    return null;
  }

  public void switchForegroundBackgroundColors() {
    Color fg = gc.getColor();
    Color bg = gc.getBackground();

    gc.setColor( bg );
    gc.setBackground( fg );
  }

  public Point getArea() {
    return area;
  }


  public static final SvgFile getNativeImage( EImage image ) {
    switch ( image ) {
      case LOCK:
        return imageLocked;
      case FAILURE:
        return imageFailure;
      case EDIT:
        return imageEdit;
      case CONTEXT_MENU:
        return imageContextMenu;
      case TRUE:
        return imageTrue;
      case FALSE:
        return imageFalse;
      case ERROR:
        return imageError;
      case INFO:
        return imageInfo;
      case TARGET:
        return imageTarget;
      case INPUT:
        return imageInput;
      case OUTPUT:
        return imageOutput;
      case ARROW:
        return imageArrow;
      case COPY_ROWS:
        return imageCopyRows;
      case LOAD_BALANCE:
        return imageLoadBalance;
      case CHECKPOINT:
        return imageCheckpoint;
      case DB:
        return imageDatabase;
      case PARALLEL:
        return imageParallel;
      case UNCONDITIONAL:
        return imageUnconditional;
      case BUSY:
        return imageBusy;
      case INJECT:
        return imageInject;
      case ARROW_DEFAULT:
        return imageArrowDefault;
      case ARROW_TRUE:
        return imageArrowTrue;
      case ARROW_FALSE:
        return imageArrowFalse;
      case ARROW_ERROR:
        return imageArrowError;
      case ARROW_DISABLED:
        return imageArrowDisabled;
      case DATA:
        return imageData;
      default:
        break;
    }
    return null;
  }

  @Override
  public void drawImage( EImage image, int x, int y, float magnification ) throws HopException {
    SvgFile svgFile = getNativeImage( image );
    drawImage( svgFile, x+xOffset, y+yOffset, miniIconSize, miniIconSize, magnification, 0 );
  }

  @Override
  public void drawImage( EImage image, int x, int y, float magnification, double angle ) throws HopException {
    SvgFile svgFile = getNativeImage( image );
    drawImage( svgFile, x + xOffset - miniIconSize / 2, y + yOffset - miniIconSize / 2, miniIconSize, miniIconSize, magnification, angle );
  }

  public void drawTransformIcon( int x, int y, TransformMeta transformMeta, float magnification ) throws HopException {

    SvgFile svgFile;    
    if ( transformMeta.isMissing() ) {
      svgFile = imageMissing;
    } else if ( transformMeta.isDeprecated() ) {
      svgFile = imageDeprecated;
    }
    else {
      String transformType = transformMeta.getTransformPluginId();
      svgFile = transformImages.get( transformType );
    }
    
    if ( svgFile != null ) { // Draw the icon!
      drawImage( svgFile, x + xOffset, y + xOffset, iconSize, iconSize, magnification, 0 );
    }
  }

  public void drawActionIcon( int x, int y, ActionMeta actionMeta, float magnification ) throws HopException {
    
    SvgFile svgFile;    
    if ( actionMeta.isMissing() ) {
      svgFile = imageMissing;
    } else if ( actionMeta.isDeprecated() ) {
      svgFile = imageDeprecated;
    }
    else {
      String actionType = actionMeta.getAction().getPluginId();
      svgFile = actionImages.get( actionType );
    }

    if ( svgFile != null ) { // Draw the icon!
      drawImage( svgFile, x + xOffset, y + xOffset, iconSize, iconSize, magnification, 0 );
    }
  }


  @Override public void drawImage( SvgFile svgFile, int x, int y, int desiredWidth, int desiredHeight, float magnification, double angle ) throws HopException {

    // Load the SVG XML document
    // Simply embed the SVG into the parent document (HopSvgGraphics2D)
    // This doesn't actually render anything, it delays that until the rendering of the whole document is done.
    //
    try {
      // Let's not hammer the file system all the time, keep the SVGDocument in memory
      //
      SvgCacheEntry cacheEntry = SvgCache.loadSvg( svgFile );
      SVGDocument svgDocument = cacheEntry.getSvgDocument();

      // How much more do we need to scale the image.
      // If the width of the icon is 500px and we desire 50px then we need to scale to 10% times the magnification
      //
      float xScaleFactor = magnification * desiredWidth / cacheEntry.getWidth();
      float yScaleFactor = magnification * desiredHeight / cacheEntry.getHeight();

      // We want to scale evenly so what's the lowest magnification?
      //
      xScaleFactor = Math.min(xScaleFactor, yScaleFactor);
      yScaleFactor = Math.min(xScaleFactor, yScaleFactor);

      gc.embedSvg(
        svgDocument.getRootElement(),
        svgFile.getFilename(),
        x-cacheEntry.getX(),        
        y-cacheEntry.getY(),
        cacheEntry.getWidth(),
        cacheEntry.getHeight(),
        xScaleFactor,
        yScaleFactor,
        Math.toDegrees( angle )
      );
    } catch ( Exception e ) {
      throw new HopException( "Unable to load SVG file '" + svgFile.getFilename() + "'", e );
    }
  }

  private void copyChildren( Document domFactory, Node target, Node svgImage ) {

    NodeList childNodes = svgImage.getChildNodes();
    for ( int c = 0; c < childNodes.getLength(); c++ ) {
      Node childNode = childNodes.item( c );

      if ( "metadata".equals( childNode.getNodeName() ) ) {
        continue; // skip some junk
      }
      if ( "defs".equals( childNode.getNodeName() ) ) {
        continue; // skip some junk
      }
      if ( "sodipodi:namedview".equals( childNode.getNodeName() ) ) {
        continue; // skip some junk
      }

      // Copy this node over to the svgSvg element
      //
      Node childNodeCopy = domFactory.importNode( childNode, true );
      target.appendChild( childNodeCopy );
    }
  }
}
