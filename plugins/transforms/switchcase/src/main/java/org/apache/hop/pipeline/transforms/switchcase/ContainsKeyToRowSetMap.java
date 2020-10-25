/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.IRowSet;

import java.util.ArrayList;
import java.util.Set;

public class ContainsKeyToRowSetMap extends KeyToRowSetMap {
  protected ArrayList<String> list = new ArrayList<>();

  protected ContainsKeyToRowSetMap() {
    super();
  }

  public Set<IRowSet> get(Object value ) {
    String valueStr = (String) value;
    for ( String key : list ) {
      if ( valueStr.contains( key ) ) {
        return super.get( key );
      }
    }
    return null;
  }

  protected void put( Object key, IRowSet rowSet ) {
    super.put( key, rowSet );
    list.add( (String) key );
  }

  public boolean containsKey( Object key ) {
    String keyStr = (String) key;
    for ( String value : list ) {
      if ( keyStr.contains( value ) ) {
        return true;
      }
    }
    return false;
  }
}
