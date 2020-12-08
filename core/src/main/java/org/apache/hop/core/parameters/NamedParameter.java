package org.apache.hop.core.parameters;

/** Target class for the parameter keys. */
  public class NamedParameter {
    /** key of this parameter */
    public String key;

    /** Description of the parameter */
    public String description;

    /** Default value for this parameter */
    public String defaultValue;

    /** Actual value of the parameter. */
    public String value;
  }