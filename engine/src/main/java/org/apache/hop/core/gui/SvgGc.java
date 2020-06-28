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

package org.apache.hop.core.gui;

import org.apache.commons.io.IOUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.SwingUniversalImage;
import org.apache.hop.core.SwingUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.core.svg.SvgSupport;
import org.apache.hop.core.util.SwingSvgImageUtil;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.action.ActionCopy;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;


public class SvgGc implements IGc {

  private static SwingUniversalImageSvg imageLocked;

  private static SwingUniversalImageSvg imageTransformError;

  private static SwingUniversalImageSvg imageEdit;

  private static SwingUniversalImageSvg imageContextMenu;

  private static SwingUniversalImageSvg imageTrue;

  private static SwingUniversalImageSvg imageFalse;

  private static SwingUniversalImageSvg imageErrorHop;

  private static SwingUniversalImageSvg imageInfoHop;

  private static SwingUniversalImageSvg imageHopTarget;

  private static SwingUniversalImageSvg imageHopInput;

  private static SwingUniversalImageSvg imageHopOutput;

  private static SwingUniversalImageSvg imageArrow;

  private static SwingUniversalImageSvg imageCopyHop;

  private static SwingUniversalImageSvg imageLoadBalance;

  private static SwingUniversalImageSvg imageCheckpoint;

  private static SwingUniversalImageSvg imageDatabase;

  private static SwingUniversalImageSvg imageParallelHop;

  private static SwingUniversalImageSvg imageUnconditionalHop;

  private static SwingUniversalImageSvg imageStart;

  private static SwingUniversalImageSvg imageDummy;

  private static SwingUniversalImageSvg imageBusy;

  private static SwingUniversalImageSvg imageInject;

  private static SwingUniversalImageSvg imageData;

  private static SwingUniversalImageSvg defaultArrow;
  private static SwingUniversalImageSvg okArrow;
  private static SwingUniversalImageSvg errorArrow;
  private static SwingUniversalImageSvg disabledArrow;

  protected Color background;

  protected Color black;
  protected Color red;
  protected Color yellow;
  protected Color orange;
  protected Color green;
  protected Color blue;
  protected Color magenta;
  protected Color gray;
  protected Color lightGray;
  protected Color darkGray;
  protected Color lightBlue;
  protected Color crystal;
  protected Color hopDefault;
  protected Color hopOK;

  private Graphics2D gc;

  private int iconSize;

  //TODO should be changed to PropsUI usage
  private int smallIconSize = 16;

  private Map<String, SwingUniversalImageSvg> transformImages;
  private Map<String, SwingUniversalImageSvg> actionImages;

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

  public SvgGc( Graphics2D gc, Point area, int iconSize, int xOffset, int yOffset ) throws HopException {
    this.gc = gc;
    this.transformImages = SwingGUIResource.getInstance().getTransformImages();
    this.actionImages = SwingGUIResource.getInstance().getActionImages();
    this.iconSize = iconSize;
    this.area = area;
    this.xOffset = xOffset;
    this.yOffset = yOffset;
    this.originalTransform = this.gc.getTransform();

    init();
  }


  private void init() throws HopException {
    this.lineStyle = ELineStyle.SOLID;
    this.lineWidth = 1;
    this.alpha = 255;

    this.background = new Color( 255, 255, 255 );
    this.black = new Color( 0, 0, 0 );
    this.red = new Color( 255, 0, 0 );
    this.yellow = new Color( 255, 255, 0 );
    this.orange = new Color( 255, 165, 0 );
    this.green = new Color( 0, 255, 0 );
    this.blue = new Color( 0, 0, 255 );
    this.magenta = new Color( 255, 0, 255 );
    this.gray = new Color( 128, 128, 128 );
    this.lightGray = new Color( 200, 200, 200 );
    this.darkGray = new Color( 80, 80, 80 );
    this.lightBlue = new Color( 135, 206, 250 ); // light sky blue
    this.crystal = new Color( 61, 99, 128 );
    this.hopDefault = new Color( 61, 99, 128 );
    this.hopOK = new Color( 12, 178, 15 );

    imageLocked = getImageIcon( BasePropertyHandler.getProperty( "Locked_image" ) );
    imageTransformError = getImageIcon( BasePropertyHandler.getProperty( "TransformErrorLines_image" ) );
    imageEdit = getImageIcon( BasePropertyHandler.getProperty( "EditSmall_image" ) );
    imageContextMenu = getImageIcon( BasePropertyHandler.getProperty( "ContextMenu_image" ) );
    imageTrue = getImageIcon( BasePropertyHandler.getProperty( "True_image" ) );
    imageFalse = getImageIcon( BasePropertyHandler.getProperty( "False_image" ) );
    imageErrorHop = getImageIcon( BasePropertyHandler.getProperty( "ErrorHop_image" ) );
    imageInfoHop = getImageIcon( BasePropertyHandler.getProperty( "InfoHop_image" ) );
    imageHopTarget = getImageIcon( BasePropertyHandler.getProperty( "HopTarget_image" ) );
    imageHopInput = getImageIcon( BasePropertyHandler.getProperty( "HopInput_image" ) );
    imageHopOutput = getImageIcon( BasePropertyHandler.getProperty( "HopOutput_image" ) );
    imageArrow = getImageIcon( BasePropertyHandler.getProperty( "ArrowIcon_image" ) );
    imageCopyHop = getImageIcon( BasePropertyHandler.getProperty( "CopyHop_image" ) );
    imageLoadBalance = getImageIcon( BasePropertyHandler.getProperty( "LoadBalance_image" ) );
    imageCheckpoint = getImageIcon( BasePropertyHandler.getProperty( "CheckeredFlag_image" ) );
    imageDatabase = getImageIcon( BasePropertyHandler.getProperty( "Database_image" ) );
    imageParallelHop = getImageIcon( BasePropertyHandler.getProperty( "ParallelHop_image" ) );
    imageUnconditionalHop = getImageIcon( BasePropertyHandler.getProperty( "UnconditionalHop_image" ) );
    imageStart = getImageIcon( BasePropertyHandler.getProperty( "STR_image" ) );
    imageDummy = getImageIcon( BasePropertyHandler.getProperty( "DUM_image" ) );
    imageBusy = getImageIcon( BasePropertyHandler.getProperty( "Busy_image" ) );
    imageInject = getImageIcon( BasePropertyHandler.getProperty( "Inject_image" ) );
    imageData = getImageIcon( BasePropertyHandler.getProperty( "Data_image" ) );

    defaultArrow = getImageIcon( BasePropertyHandler.getProperty( "defaultArrow_image" ) );
    okArrow = getImageIcon( BasePropertyHandler.getProperty( "okArrow_image" ) );
    errorArrow = getImageIcon( BasePropertyHandler.getProperty( "errorArrow_image" ) );
    disabledArrow = getImageIcon( BasePropertyHandler.getProperty( "disabledArrow_image" ) );

    fontGraph = new Font( "FreeSans", Font.PLAIN, 10 );
    fontNote = new Font( "FreeSans", Font.PLAIN, 10 );
    fontSmall = new Font( "FreeSans", Font.PLAIN, 8 );

    gc.setFont( fontGraph );

    gc.setColor( background );
    gc.fillRect( 0, 0, area.x, area.y );
  }

  private SwingUniversalImageSvg getImageIcon( String fileName ) throws HopException {
    SwingUniversalImageSvg image = null;

    InputStream inputStream = null;
    if ( fileName == null ) {
      throw new HopException( "Image icon file name can not be null" );
    }

    if ( SvgSupport.isSvgEnabled() && SvgSupport.isSvgName( fileName ) ) {
      try {
        inputStream = new FileInputStream( fileName );
      } catch ( FileNotFoundException ex ) {
        // no need to fail
      }
      if ( inputStream == null ) {
        try {
          inputStream = new FileInputStream( "/" + fileName );
        } catch ( FileNotFoundException ex ) {
          // no need to fail
        }
      }
      if ( inputStream == null ) {
        inputStream = getClass().getResourceAsStream( fileName );
      }
      if ( inputStream == null ) {
        inputStream = getClass().getResourceAsStream( "/" + fileName );
      }
      if ( inputStream != null ) {
        try {
          SvgImage svg = SvgSupport.loadSvgImage( inputStream );
          image = new SwingUniversalImageSvg( svg );
        } catch ( Exception ex ) {
          throw new HopException( "Unable to load image from classpath : '" + fileName + "'", ex );
        } finally {
          IOUtils.closeQuietly( inputStream );
        }
      }
    }

    if ( image == null ) {
      throw new HopException( "Unable to load image from classpath : '" + fileName + "'" );
    }

    return image;
  }

  public void dispose() {
  }

  public void drawLine( int x, int y, int x2, int y2 ) {
    gc.drawLine( x + xOffset, y + yOffset, x2 + xOffset, y2 + yOffset );
  }

  @Override
  public void drawImage( String location, ClassLoader classLoader, int x, int y ) {
    SwingUniversalImageSvg img = SwingSvgImageUtil.getUniversalImage( classLoader, location );
    drawImage( img, x, y, smallIconSize );
  }

  @Override
  public void drawImage( EImage image, int x, int y ) {
    drawImage( image, x, y, 0.0f );
  }

  @Override
  public void drawImage( EImage image, int locationX, int locationY, float magnification ) {
    SwingUniversalImage img = getNativeImage( image );
    drawImage( img, locationX, locationY, smallIconSize );
  }

  public void drawImage( EImage image, int x, int y, int width, int height, float magnification ) {
    SwingUniversalImage img = getNativeImage( image );
    drawImage( img, x, y, width, height );
  }

  @Override
  public void drawImage( EImage image, int x, int y, float magnification, double angle ) {
    SwingUniversalImage img = getNativeImage( image );
    drawImage( img, x, y, angle, smallIconSize );
  }

  protected void drawImage( SwingUniversalImage image, int locationX, int locationY, int imageSize ) {
    image.drawToGraphics( gc, locationX, locationY, imageSize, imageSize );
  }

  protected void drawImage( SwingUniversalImage image, int centerX, int centerY, double angle, int imageSize ) {
    image.drawToGraphics( gc, centerX, centerY, imageSize, imageSize, angle );
  }

  public Point getImageBounds( EImage image ) {
    return new Point( smallIconSize, smallIconSize );
  }

  public static final SwingUniversalImageSvg getNativeImage( EImage image ) {
    switch ( image ) {
      case LOCK:
        return imageLocked;
      case TRANSFORM_ERROR:
        return imageTransformError;
      case EDIT:
        return imageEdit;
      case CONTEXT_MENU:
        return imageContextMenu;
      case TRUE:
        return imageTrue;
      case FALSE:
        return imageFalse;
      case ERROR:
        return imageErrorHop;
      case INFO:
        return imageInfoHop;
      case TARGET:
        return imageHopTarget;
      case INPUT:
        return imageHopInput;
      case OUTPUT:
        return imageHopOutput;
      case ARROW:
        return imageArrow;
      case COPY_ROWS:
        return imageCopyHop;
      case LOAD_BALANCE:
        return imageLoadBalance;
      case CHECKPOINT:
        return imageCheckpoint;
      case DB:
        return imageDatabase;
      case PARALLEL:
        return imageParallelHop;
      case UNCONDITIONAL:
        return imageUnconditionalHop;
      case BUSY:
        return imageBusy;
      case INJECT:
        return imageInject;
      case ARROW_DEFAULT:
        return defaultArrow;
      case ARROW_OK:
        return okArrow;
      case ARROW_ERROR:
        return errorArrow;
      case ARROW_DISABLED:
        return disabledArrow;
      case DATA:
        return imageData;
      default:
        break;
    }
    return null;
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

    String[] lines = text.split( "\n" );
    for ( String line : lines ) {
      gc.drawString( line, x + xOffset, y + height + yOffset );
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
      case ORANGE:
        return orange;
      case GREEN:
        return green;
      case BLUE:
        return blue;
      case MAGENTA:
        return magenta;
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
      case HOP_OK:
        return hopOK;
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

  public void drawTransformIcon( int x, int y, TransformMeta transformMeta, float magnification ) {
    String transformType = transformMeta.getTransformPluginId();
    SwingUniversalImageSvg im = transformImages.get( transformType );
    if ( im != null ) { // Draw the icon!
      drawImage( im, x + xOffset, y + xOffset, iconSize );
    }
  }

  public void drawActionIcon( int x, int y, ActionCopy actionCopy, float magnification ) {
    if ( actionCopy == null ) {
      return; // Don't draw anything
    }

    SwingUniversalImage image = null;

    if ( actionCopy.isSpecial() ) {
      if ( actionCopy.isStart() ) {
        image = imageStart;
      }
      if ( actionCopy.isDummy() ) {
        image = imageDummy;
      }
    } else {
      String configId = actionCopy.getAction().getPluginId();
      if ( configId != null ) {
        image = actionImages.get( configId );
      }
    }
    if ( image == null ) {
      return;
    }

    drawImage( image, x + xOffset, y + xOffset, iconSize );
  }

  @Override
  public void drawActionIcon( int x, int y, ActionCopy actionCopy ) {
    drawActionIcon( x, y, actionCopy, 1.0f );
  }

  @Override
  public void drawTransformIcon( int x, int y, TransformMeta transformMeta ) {
    drawTransformIcon( x, y, transformMeta, 1.0f );
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

  @Override
  public void drawImage( BufferedImage image, int x, int y ) {
    // Do NOT draw bitmaps
    //
  }
}
