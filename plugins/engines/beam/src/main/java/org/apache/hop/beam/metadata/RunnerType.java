package org.apache.hop.beam.metadata;

public enum RunnerType {
  Direct,
  DataFlow,
  Spark,
  Flink,
  ;

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i=0;i<names.length;i++) {
      names[i] = values()[i].name();
    }
    return names;
  }

  /**
   * Find the runner type by name.  If not found or if the name is null, return null.
   *
   * @param name
   * @return
   */
  public static RunnerType getRunnerTypeByName( String name) {
    if (name==null) {
      return null;
    }
    for ( RunnerType type : values()) {
      if (type.name().equalsIgnoreCase( name )) {
        return type;
      }
    }
    return null;
  }

}
