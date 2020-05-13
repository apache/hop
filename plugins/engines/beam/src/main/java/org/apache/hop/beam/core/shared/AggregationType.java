package org.apache.hop.beam.core.shared;

import org.apache.hop.core.exception.HopException;

public enum AggregationType {
  SUM, AVERAGE, COUNT_ALL, MIN, MAX, FIRST_INCL_NULL, LAST_INCL_NULL, FIRST, LAST,
  ;

  public static final AggregationType getTypeFromName( String name) throws HopException {
    for ( AggregationType type : values()) {
      if (name.equals( type.name() )) {
        return type;
      }
    }
    throw new HopException( "Aggregation type '"+name+"' is not recognized or supported" );
  }
}
