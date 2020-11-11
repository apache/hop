package org.apache.hop.base;

import org.apache.hop.core.listeners.IFilenameChangedListener;

import java.util.Objects;

public class MockFilenameChangeListener implements IFilenameChangedListener {

  private int id;

  public MockFilenameChangeListener( int id ) {
    this.id = id;
  }

  @Override public void filenameChanged( Object object, String oldFilename, String newFilename ) {
    // Nothing
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    MockFilenameChangeListener that = (MockFilenameChangeListener) o;
    return id == that.id;
  }

  @Override public int hashCode() {
    return Objects.hash( id );
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public int getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( int id ) {
    this.id = id;
  }
}
