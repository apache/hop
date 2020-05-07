package org.apache.hop.beam.transforms.kafka;

public class ConfigOption {

  public enum Type {

    String, Short, Int, Long, Double, Boolean,
    ;

    public static final Type getTypeFromName(String name) {
      for (Type type : values()) {
        if (type.name().equalsIgnoreCase( name )) {
          return type;
        }
      }
      return String;
    }

    public static final String[] getTypeNames() {
      String[] names = new String[values().length];
      for (int i=0;i<names.length;i++) {
        names[ i ] = values()[i].name();
      }
      return names;
    }
  }

  private String parameter;
  private String value;
  private Type type;

  public ConfigOption() {
  }

  public ConfigOption( String parameter, String value, Type type ) {
    this.parameter = parameter;
    this.value = value;
    this.type = type;
  }

  /**
   * Gets parameter
   *
   * @return value of parameter
   */
  public String getParameter() {
    return parameter;
  }

  /**
   * @param parameter The parameter to set
   */
  public void setParameter( String parameter ) {
    this.parameter = parameter;
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
   * Gets type
   *
   * @return value of type
   */
  public Type getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( Type type ) {
    this.type = type;
  }
}
