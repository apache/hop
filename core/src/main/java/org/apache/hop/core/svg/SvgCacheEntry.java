package org.apache.hop.core.svg;

import org.w3c.dom.svg.SVGDocument;

import java.util.Objects;

public class SvgCacheEntry {

  private String filename;
  private SVGDocument svgDocument;
  private int width;
  private int height;

  public SvgCacheEntry( String filename, SVGDocument svgDocument, int width, int height ) {
    this.filename = filename;
    this.svgDocument = svgDocument;
    this.width = width;
    this.height = height;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    SvgCacheEntry that = (SvgCacheEntry) o;
    return Objects.equals( filename, that.filename );
  }

  @Override public int hashCode() {
    return Objects.hash( filename );
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * Gets svgDocument
   *
   * @return value of svgDocument
   */
  public SVGDocument getSvgDocument() {
    return svgDocument;
  }

  /**
   * @param svgDocument The svgDocument to set
   */
  public void setSvgDocument( SVGDocument svgDocument ) {
    this.svgDocument = svgDocument;
  }

  /**
   * Gets width
   *
   * @return value of width
   */
  public int getWidth() {
    return width;
  }

  /**
   * @param width The width to set
   */
  public void setWidth( int width ) {
    this.width = width;
  }

  /**
   * Gets height
   *
   * @return value of height
   */
  public int getHeight() {
    return height;
  }

  /**
   * @param height The height to set
   */
  public void setHeight( int height ) {
    this.height = height;
  }
}
