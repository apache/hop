package org.apache.hop.testing;

import org.apache.hop.metastore.persist.MetaStoreAttribute;

public class VariableValue {
  @MetaStoreAttribute
  private String key;

  @MetaStoreAttribute
  private String value;

  public VariableValue() {
  }

  public VariableValue( String key, String value ) {
    this.key = key;
    this.value = value;
  }

  /**
   * Gets key
   *
   * @return value of key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key The key to set
   */
  public void setKey( String key ) {
    this.key = key;
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
}
