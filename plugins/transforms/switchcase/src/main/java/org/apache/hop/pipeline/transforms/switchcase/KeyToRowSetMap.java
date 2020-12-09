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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.IRowSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KeyToRowSetMap {

  protected Map<Object, Set<IRowSet>> map;

  protected KeyToRowSetMap() {
    map = new HashMap<>();
  }

  /**
   * Support custom runtime implementation.
   *
   * @param map
   */
  protected KeyToRowSetMap( Map<Object, Set<IRowSet>> map ) {
    this.map = map;
  }

  protected Set<IRowSet> get( Object key ) {
    return map.get( key );
  }

  protected void put( Object key, IRowSet rowSet ) {
    Set<IRowSet> existing = map.get( key );
    if ( existing == null ) {
      existing = new HashSet<>();
      map.put( key, existing );
    }
    existing.add( rowSet );
  }

  public boolean containsKey( Object key ) {
    return map.containsKey( key );
  }

  public boolean isEmpty() {
    return map.keySet().isEmpty();
  }

  protected Set<Object> keySet() {
    return map.keySet();
  }

  protected Set<Map.Entry<Object, Set<IRowSet>>> entrySet() {
    return map.entrySet();
  }
}
