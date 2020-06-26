package org.apache.hop.core.config;

import java.util.Objects;

public class DescribedVariable {

  private String name;

  private String value;

  private String description;

  public DescribedVariable() {
  }

  public DescribedVariable( String name, String value, String description ) {
    this.name = name;
    this.value = value;
    this.description = description;
  }

  public DescribedVariable( DescribedVariable e) {
    this.name = e.name;
    this.value = e.value;
    this.description = e.description;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    DescribedVariable that = (DescribedVariable) o;
    return Objects.equals( name, that.name );
  }

  @Override public int hashCode() {
    return Objects.hash( name );
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets value
   *
   * @return value of value
   */
  public String getValue() {
    return value;
  }

  /**
   * @param value The value to set
   */
  public void setValue( String value ) {
    this.value = value;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }
}
