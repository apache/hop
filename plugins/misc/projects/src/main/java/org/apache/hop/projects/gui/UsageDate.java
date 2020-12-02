package org.apache.hop.projects.gui;

import java.util.Date;
import java.util.Objects;

public class UsageDate {
  private String name;
  private Date date;

  public UsageDate( String name, Date date ) {
    this.name = name;
    this.date = date;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    UsageDate usageDate = (UsageDate) o;
    return Objects.equals( name, usageDate.name );
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
   * Gets date
   *
   * @return value of date
   */
  public Date getDate() {
    return date;
  }

  /**
   * @param date The date to set
   */
  public void setDate( Date date ) {
    this.date = date;
  }
}
