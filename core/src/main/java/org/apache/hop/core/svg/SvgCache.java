package org.apache.hop.core.svg;

import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.w3c.dom.Element;
import org.w3c.dom.svg.SVGDocument;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SvgCache {
  private static SvgCache instance;

  private Map<String, SvgCacheEntry> fileDocumentMap;

  private SvgCache() {
    fileDocumentMap = new HashMap<>();
  }

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static SvgCache getInstance() {
    if ( instance == null ) {
      instance = new SvgCache();
    }
    return instance;
  }

  public synchronized static SvgCacheEntry findSvg( String filename ) {
    return getInstance().fileDocumentMap.get( filename );
  }

  public synchronized static SvgCacheEntry loadSvg( SvgFile svgFile ) throws HopException {

    SvgCacheEntry cacheEntry = findSvg( svgFile.getFilename() );
    if (cacheEntry!=null) {
      return cacheEntry;
    }

    try {
      String parser = XMLResourceDescriptor.getXMLParserClassName();
      SAXSVGDocumentFactory factory = new SAXSVGDocumentFactory( parser );
      InputStream svgStream = svgFile.getClassLoader().getResourceAsStream( svgFile.getFilename() );

      if ( svgStream == null ) {
        throw new HopException( "Unable to find file '" + svgFile.getFilename() + "'" );
      }
      SVGDocument svgDocument = factory.createSVGDocument( svgFile.getFilename(), svgStream );

      Element elSVG = svgDocument.getRootElement();
      String widthString = elSVG.getAttribute( "width" );
      String heightString = elSVG.getAttribute( "height" );
      double width = Const.toDouble( widthString.replace( "px", "" ), -1 );
      double height = Const.toDouble( heightString.replace( "px", "" ), -1 );
      if ( width < 0 || height < 0 ) {
        throw new HopException( "Unable to find valid width or height in SVG document " + svgFile.getFilename() );
      }
      cacheEntry = new SvgCacheEntry( svgFile.getFilename(), svgDocument, (int)Math.round(width), (int)Math.round(height) );
      getInstance().fileDocumentMap.put( svgFile.getFilename(), cacheEntry );
      return cacheEntry;
    } catch ( Exception e ) {
      throw new HopException( "Error loading SVG file " + svgFile.getFilename(), e );
    }
  }

  public synchronized static void addSvg( String filename, SVGDocument svgDocument, int width, int height ) {
    getInstance().fileDocumentMap.put( filename, new SvgCacheEntry( filename, svgDocument, width, height ) );
  }

}
