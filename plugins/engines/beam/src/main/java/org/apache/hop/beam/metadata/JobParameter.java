package org.apache.hop.beam.metadata;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class JobParameter {

  @HopMetadataProperty
  private String variable;

  @HopMetadataProperty
  private String value;

  public JobParameter() {
  }

  public JobParameter( String variable, String value ) {
    this.variable = variable;
    this.value = value;
  }

  /**
   * Gets variable
   *
   * @return value of variable
   */
  public String getVariable() {
    return variable;
  }

  /**
   * @param variable The variable to set
   */
  public void setVariable( String variable ) {
    this.variable = variable;
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
