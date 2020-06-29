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

import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.util.SVGConstants;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.svg.SVGDocument;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import static org.apache.batik.svggen.DOMGroupManager.DRAW;


public class SvgGc implements IGc {

  private static SvgFile imageLocked;

  private static SvgFile imageTransformError;

  private static SvgFile imageEdit;

  private static SvgFile imageContextMenu;

  private static SvgFile imageTrue;

  private static SvgFile imageFalse;

  private static SvgFile imageErrorHop;

  private static SvgFile imageInfoHop;

  private static SvgFile imageHopTarget;

  private static SvgFile imageHopInput;

  private static SvgFile imageHopOutput;

  private static SvgFile imageArrow;

  private static SvgFile imageCopyHop;

  private static SvgFile imageLoadBalance;

  private static SvgFile imageCheckpoint;

  private static SvgFile imageDatabase;

  private static SvgFile imageParallelHop;

  private static SvgFile imageUnconditionalHop;

  private static SvgFile imageStart;

  private static SvgFile imageDummy;

  private static SvgFile imageBusy;

  private static SvgFile imageInject;

  private static SvgFile imageData;

  private static SvgFile defaultArrow;
  private static SvgFile okArrow;
  private static SvgFile errorArrow;
  private static SvgFile disabledArrow;

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
    this.miniIconSize = iconSize/2;
    this.area = area;
    this.xOffset = xOffset;
    this.yOffset = yOffset;
    this.originalTransform = this.gc.getTransform();

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

    for ( IPlugin plugin : registry.getPlugins( TransformPluginType.class ) ) {
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

    imageLocked = new SvgFile( BasePropertyHandler.getProperty( "Locked_image" ), this.getClass().getClassLoader() );
    imageTransformError = new SvgFile( BasePropertyHandler.getProperty( "TransformErrorLines_image" ), this.getClass().getClassLoader() );
    imageEdit = new SvgFile( BasePropertyHandler.getProperty( "EditSmall_image" ), this.getClass().getClassLoader() );
    imageContextMenu = new SvgFile( BasePropertyHandler.getProperty( "ContextMenu_image" ), this.getClass().getClassLoader() );
    imageTrue = new SvgFile( BasePropertyHandler.getProperty( "True_image" ), this.getClass().getClassLoader() );
    imageFalse = new SvgFile( BasePropertyHandler.getProperty( "False_image" ), this.getClass().getClassLoader() );
    imageErrorHop = new SvgFile( BasePropertyHandler.getProperty( "ErrorHop_image" ), this.getClass().getClassLoader() );
    imageInfoHop = new SvgFile( BasePropertyHandler.getProperty( "InfoHop_image" ), this.getClass().getClassLoader() );
    imageHopTarget = new SvgFile( BasePropertyHandler.getProperty( "HopTarget_image" ), this.getClass().getClassLoader() );
    imageHopInput = new SvgFile( BasePropertyHandler.getProperty( "HopInput_image" ), this.getClass().getClassLoader() );
    imageHopOutput = new SvgFile( BasePropertyHandler.getProperty( "HopOutput_image" ), this.getClass().getClassLoader() );
    imageArrow = new SvgFile( BasePropertyHandler.getProperty( "ArrowIcon_image" ), this.getClass().getClassLoader() );
    imageCopyHop = new SvgFile( BasePropertyHandler.getProperty( "CopyHop_image" ), this.getClass().getClassLoader() );
    imageLoadBalance = new SvgFile( BasePropertyHandler.getProperty( "LoadBalance_image" ), this.getClass().getClassLoader() );
    imageCheckpoint = new SvgFile( BasePropertyHandler.getProperty( "CheckeredFlag_image" ), this.getClass().getClassLoader() );
    imageDatabase = new SvgFile( BasePropertyHandler.getProperty( "Database_image" ), this.getClass().getClassLoader() );
    imageParallelHop = new SvgFile( BasePropertyHandler.getProperty( "ParallelHop_image" ), this.getClass().getClassLoader() );
    imageUnconditionalHop = new SvgFile( BasePropertyHandler.getProperty( "UnconditionalHop_image" ), this.getClass().getClassLoader() );
    imageStart = new SvgFile( BasePropertyHandler.getProperty( "STR_image" ), this.getClass().getClassLoader() );
    imageDummy = new SvgFile( BasePropertyHandler.getProperty( "DUM_image" ), this.getClass().getClassLoader() );
    imageBusy = new SvgFile( BasePropertyHandler.getProperty( "Busy_image" ), this.getClass().getClassLoader() );
    imageInject = new SvgFile( BasePropertyHandler.getProperty( "Inject_image" ), this.getClass().getClassLoader() );
    imageData = new SvgFile( BasePropertyHandler.getProperty( "Data_image" ), this.getClass().getClassLoader() );

    defaultArrow = new SvgFile( BasePropertyHandler.getProperty( "defaultArrow_image" ), this.getClass().getClassLoader() );
    okArrow = new SvgFile( BasePropertyHandler.getProperty( "okArrow_image" ), this.getClass().getClassLoader() );
    errorArrow = new SvgFile( BasePropertyHandler.getProperty( "errorArrow_image" ), this.getClass().getClassLoader() );
    disabledArrow = new SvgFile( BasePropertyHandler.getProperty( "disabledArrow_image" ), this.getClass().getClassLoader() );

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

  @Override
  public void drawImage( EImage image, int locationX, int locationY, float magnification ) throws HopException {
    drawImage( image, locationX, locationY, magnification, 0 );
  }

  @Override
  public void drawImage( EImage image, int x, int y, float magnification, double angle ) throws HopException {
    SvgFile svgFile = getNativeImage( image );
    drawImage( svgFile, x, y, miniIconSize, miniIconSize, magnification, angle );
  }

  public static final SvgFile getNativeImage( EImage image ) {
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

  public void drawTransformIcon( int x, int y, TransformMeta transformMeta, float magnification ) throws HopException {
    String transformType = transformMeta.getTransformPluginId();
    SvgFile svgFile = transformImages.get( transformType );
    if ( svgFile != null ) { // Draw the icon!
      drawImage( svgFile, x + xOffset, y + xOffset, iconSize, iconSize, magnification, 0 );
    }
  }

  public void drawActionIcon( int x, int y, ActionCopy actionCopy, float magnification ) throws HopException {
    if ( actionCopy == null ) {
      return; // Don't draw anything
    }

    SvgFile svgFile = null;

    if ( actionCopy.isSpecial() ) {
      if ( actionCopy.isStart() ) {
        svgFile = imageStart;
      }
      if ( actionCopy.isDummy() ) {
        svgFile = imageDummy;
      }
    } else {
      String configId = actionCopy.getAction().getPluginId();
      if ( configId != null ) {
        svgFile = actionImages.get( configId );
      }
    }
    if ( svgFile == null ) {
      return;
    }

    drawImage( svgFile, x + xOffset, y + xOffset, iconSize, iconSize, magnification, 0 );
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


  @Override public void drawImage( SvgFile svgFile, int x, int y, int desiredWidth, int desiredHeight, float magnification, double angle ) throws HopException {

    // Load the SVG XML document
    // Simply embed the SVG into the parent document (HopSvgGraphics2D)
    // This doesn't actually render anything, it delays that until the rendering of the whole document is done.
    //
    AffineTransform oldTransform = gc.getTransform();
    gc.scale( magnification, magnification );
    gc.rotate( angle );

    // Don't scale the location...
    int imageX = (int) ( x / magnification );
    int imageY = (int) ( y / magnification );

    try {
      try {

        SVGDocument svgDocument;
        int width;
        int height;

        // Let's not hammer the file system all the time, keep the SVGDocument in memory
        //
        SvgCacheEntry cacheEntry = SvgCache.findSvg( svgFile.getFilename() );
        if ( cacheEntry == null ) {
          String parser = XMLResourceDescriptor.getXMLParserClassName();
          SAXSVGDocumentFactory factory = new SAXSVGDocumentFactory( parser );
          InputStream svgStream = svgFile.getClassLoader().getResourceAsStream( svgFile.getFilename() );

          if ( svgStream == null ) {
            throw new HopException( "Unable to find file '" + svgFile.getFilename() + "'" );
          }
          svgDocument = factory.createSVGDocument( svgFile.getFilename(), svgStream );

          Element elSVG = svgDocument.getRootElement();
          String widthString = elSVG.getAttribute( "width" );
          String heightString = elSVG.getAttribute( "height" );
          width = Const.toInt( widthString.replace( "px", "" ), -1 );
          height = Const.toInt( heightString.replace( "px", "" ), -1 );
          if ( width < 0 || height < 0 ) {
            throw new HopException( "Unable to find valid width or height in SVG document " + svgFile.getFilename() );
          }
          SvgCache.addSvg( svgFile.getFilename(), svgDocument, width, height );
        } else {
          svgDocument = cacheEntry.getSvgDocument();
          width = cacheEntry.getWidth();
          height = cacheEntry.getHeight();
        }

        // How much more do we need to scale the image.
        // If the Width of the icon is 500px and we desire 50px then we need to scale to 10% times the magnification
        //
        float xScaleFactor = magnification * (desiredWidth / width);
        String xScalePercent = new DecimalFormat( "##0'%'" ).format( xScaleFactor * 100 );

        float yScaleFactor = magnification * (desiredHeight / height);
        String yScalePercent = new DecimalFormat( "##0'%'" ).format( yScaleFactor * 100 );
        
        Document domFactory = gc.getDOMFactory();

        // Simply embed the image SVG
        //
        Element svgSvg = domFactory.createElementNS( SVGConstants.SVG_NAMESPACE_URI, SVGConstants.SVG_SVG_TAG );

        svgSvg.setAttributeNS( null, SVGConstants.SVG_X_ATTRIBUTE, Integer.toString( imageX ) );
        svgSvg.setAttributeNS( null, SVGConstants.SVG_Y_ATTRIBUTE, Integer.toString( imageY ) );
        svgSvg.setAttributeNS( null, SVGConstants.SVG_WIDTH_ATTRIBUTE, Integer.toString( width ) );
        svgSvg.setAttributeNS( null, SVGConstants.SVG_HEIGHT_ATTRIBUTE, Integer.toString( height ) );
        svgSvg.setAttributeNS( null, SVGConstants.SVG_TRANSFORM_ATTRIBUTE, xScalePercent + " " + yScalePercent );

        // Add all the elements from the SVG Image...
        //
        NodeList childNodes = svgDocument.getRootElement().getChildNodes();
        for ( int c = 0; c < childNodes.getLength(); c++ ) {
          Node childNode = childNodes.item( c );

          // Copy this node over to the svgSvg element
          //
          Node childNodeCopy = domFactory.importNode( childNode, true );
          svgSvg.appendChild( childNodeCopy );
        }

        gc.getDomGroupManager().addElement( svgSvg, DRAW );

      } catch ( IOException e ) {
        throw new HopException( "Error reading file '" + svgFile.getFilename() + "'", e );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to load SVG file '" + svgFile.getFilename() + "'", e );
    } finally {
      // Reset scaling
      //
      gc.setTransform( oldTransform );
    }
  }

}
