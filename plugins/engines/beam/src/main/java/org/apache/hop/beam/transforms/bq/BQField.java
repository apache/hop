package org.apache.hop.beam.transforms.bq;

import org.apache.commons.lang.StringUtils;

public class BQField {
  private String name;
  private String newName;
  private String kettleType;

  public BQField() {
  }

  public BQField( String name, String newName, String kettleType ) {
    this.name = name;
    this.newName = newName;
    this.kettleType = kettleType;
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
   * Gets kettleType
   *
   * @return value of kettleType
   */
  public String getKettleType() {
    return kettleType;
  }

  /**
   * @param kettleType The kettleType to set
   */
  public void setKettleType( String kettleType ) {
    this.kettleType = kettleType;
  }
}
