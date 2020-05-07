package org.apache.hop.beam.core.shared;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A variablespace which simply is easy to serialize.
 */
public class SimpleVariableSpace implements Serializable {

  private List<VariableValue> variables;

  public SimpleVariableSpace() {
    variables = new ArrayList<>();
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public List<VariableValue> getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables( List<VariableValue> variables ) {
    this.variables = variables;
  }
}
