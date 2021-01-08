/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
