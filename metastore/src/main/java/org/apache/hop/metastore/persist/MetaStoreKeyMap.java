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

package org.apache.hop.metastore.persist;

import java.util.HashMap;
import java.util.Map;

/**
 * This class holds the metastore id values used for migrating metastore element ids
 */
public class MetaStoreKeyMap {

  public static Map<String, String[]> keyMap = new HashMap<String, String[]>();

  static {
    keyMap.put( "host_name", new String[]{ "hostname" } );
    keyMap.put( "server_name", new String[]{ "servername" } );
    keyMap.put( "step_name", new String[]{ "stepname", "stepName" } );
    keyMap.put( "field_mappings", new String[]{ "fieldMappings" } );
    keyMap.put( "parameter_name", new String[]{ "parameterName" }  );
    keyMap.put( "source_field_name", new String[]{ "sourceFieldName" } );
    keyMap.put( "target_field_name", new String[]{ "targetFieldName" } );
  }

  public static String[] get( String key ) {
    String[] keys = keyMap.get( key );

    if ( keys != null ) {
      return keys;
    }

    return new String[ 0 ];
  }
}
