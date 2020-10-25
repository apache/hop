/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the counters for Hop, the pipelines, workflows, ...
 *
 * @author Matt
 * @since 17-apr-2005
 */
public class Counters {
  private static Counters counters = null;
  private Map<String, Counter> counterMap = null;

  private Counters() {
    counterMap = Collections.synchronizedMap(new HashMap<>());
  }

  public static final Counters getInstance() {
    if ( counters != null ) {
      return counters;
    }
    counters = new Counters();
    return counters;
  }

  public Counter getCounter( String name ) {
    return counterMap.get( name );
  }

  public void setCounter( String name, Counter counter ) {
    counterMap.put( name, counter );
  }

  public void clearCounter( String name ) {
    counterMap.remove( name );
  }

  public void clear() {
    counterMap.clear();
  }

  /**
   * Gets counterTable
   *
   * @return value of counterTable
   */
  public Map<String, Counter> getCounterMap() {
    return counterMap;
  }
}
