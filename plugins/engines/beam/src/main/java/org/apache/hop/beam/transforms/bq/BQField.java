package org.apache.hop.beam.transforms.bq;

import org.apache.commons.lang.StringUtils;

public class BQField {
  private String name;
  private String newName;
  private String hopType;

  public BQField() {
  }

  public BQField( String name, String newName, String hopType ) {
    this.name = name;
    this.newName = newName;
    this.hopType = hopType;
  }

  public String getNewNameOrName() {
    if ( StringUtils.isNotEmpty(newName)) {
      return newName;
    } else {
      return name;
    }
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
   * Gets newName
   *
   * @return value of newName
   */
  public String getNewName() {
    return newName;
  }

  /**
   * @param newName The newName to set
   */
  public void setNewName( String newName ) {
    this.newName = newName;
  }

  /**
   * Gets hopType
   *
   * @return value of hopType
   */
  public String getHopType() {
    return hopType;
  }

  /**
   * @param hopType The hopType to set
   */
  public void setHopType( String hopType ) {
    this.hopType = hopType;
  }
}
