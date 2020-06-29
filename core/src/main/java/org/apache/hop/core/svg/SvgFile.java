package org.apache.hop.core.svg;

import java.util.Objects;

public class SvgFile {
  private String filename;
  private ClassLoader classLoader;

  public SvgFile( String filename, ClassLoader classLoader ) {
    this.filename = filename;
    this.classLoader = classLoader;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    SvgFile svgFile = (SvgFile) o;
    return Objects.equals( filename, svgFile.filename );
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
   * Gets classLoader
   *
   * @return value of classLoader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * @param classLoader The classLoader to set
   */
  public void setClassLoader( ClassLoader classLoader ) {
    this.classLoader = classLoader;
  }
}
