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

package org.apache.hop.pipeline.transform.utils;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;

public class RowMetaUtils {

  public static IRowMeta getRowMetaForUpdate( IRowMeta prev, String[] keyLookup, String[] keyStream,
                                              String[] updateLookup, String[] updateStream ) throws HopTransformException {
    IRowMeta tableFields = new RowMeta();

    // Now change the field names
    // the key fields
    if ( keyLookup != null ) {
      for ( int i = 0; i < keyLookup.length; i++ ) {
        IValueMeta v = prev.searchValueMeta( keyStream[ i ] );
        if ( v != null ) {
          IValueMeta tableField = v.clone();
          tableField.setName( keyLookup[ i ] );
          tableFields.addValueMeta( tableField );
        } else {
          throw new HopTransformException( "Unable to find field [" + keyStream[ i ] + "] in the input rows" );
        }
      }
    }
    // the lookup fields
    for ( int i = 0; i < updateLookup.length; i++ ) {
      IValueMeta v = prev.searchValueMeta( updateStream[ i ] );
      if ( v != null ) {
        IValueMeta vk = tableFields.searchValueMeta( updateLookup[ i ] );
        if ( vk == null ) { // do not add again when already added as key fields
          IValueMeta tableField = v.clone();
          tableField.setName( updateLookup[ i ] );
          tableFields.addValueMeta( tableField );
        }
      } else {
        throw new HopTransformException( "Unable to find field [" + updateStream[ i ] + "] in the input rows" );
      }
    }
    return tableFields;
  }

}
