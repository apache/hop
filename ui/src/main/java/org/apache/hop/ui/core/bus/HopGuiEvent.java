package org.apache.hop.ui.core.bus;

import java.util.Objects;

public class HopGuiEvent<T> {

  private String id;
  private T subject;

  public HopGuiEvent( String id, T subject ) {
    this.id = id;
    this.subject = subject;
  }

  public HopGuiEvent( String id ) {
    this(id, null);
  }

  @Override public String toString() {
    return "HopGuiEvent{" +
      "id='" + id + '\'' +
      '}';
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    HopGuiEvent<?> that = (HopGuiEvent<?>) o;
    return Objects.equals( id, that.id ) &&
      Objects.equals( subject, that.subject );
  }

  @Override public int hashCode() {
    return Objects.hash( id, subject );
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * Gets subject
   *
   * @return value of subject
   */
  public T getSubject() {
    return subject;
  }

  /**
   * @param subject The subject to set
   */
  public void setSubject( T subject ) {
    this.subject = subject;
  }
}
