package org.apache.hop.beam.core.shared;

import java.io.Serializable;

public class VariableValue implements Serializable {

  private String variable;
  private String value;

  public VariableValue() {
  }

  public VariableValue( String variable, String value ) {
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
