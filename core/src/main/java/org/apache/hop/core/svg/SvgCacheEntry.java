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

package org.apache.hop.core.svg;

import org.w3c.dom.svg.SVGDocument;

import java.util.Objects;

public class SvgCacheEntry {

  private SVGDocument svgDocument;
  private float width;
  private float height;
  private int x;
  private int y;
  private String filename;

  public SvgCacheEntry( String filename, SVGDocument svgDocument, int width, int height, int x, int y ) {
    this.filename = filename;
    this.svgDocument = svgDocument;
    this.width = width;
    this.height = height;
    this.x = x;
    this.y = y;
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
  public float getWidth() {
    return width;
  }

  /**
   * @param width The width to set
   */
  public void setWidth( float width ) {
    this.width = width;
  }

  /**
   * Gets height
   *
   * @return value of height
   */
  public float getHeight() {
    return height;
  }

  /**
   * @param height The height to set
   */
  public void setHeight( float height ) {
    this.height = height;
  }

  /**
   * Gets x
   *
   * @return value of x
   */
  public int getX() {
    return x;
  }

  /**
   * @param x The x to set
   */
  public void setX( int x ) {
    this.x = x;
  }

  /**
   * Gets y
   *
   * @return value of y
   */
  public int getY() {
    return y;
  }

  /**
   * @param y The y to set
   */
  public void setY( int y ) {
    this.y = y;
  }
}
