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

package org.apache.hop.pipeline.transforms.uniquerowsbyhashset;

import java.util.Arrays;

// Package private
class RowKey {
  // TODO: This field needs to be set by a checkbox in the transform dialog.
  private boolean storeValues;
  private int hash;
  private Object[] storedFieldValues;

  public RowKey( Object[] row, UniqueRowsByHashSetData sdi ) {
    Object[] keyFields;
    // If we are keying on the entire row
    if ( sdi.fieldnrs.length == 0 ) {
      keyFields = row;
    } else {
      keyFields = new Object[ sdi.fieldnrs.length ];
      for ( int i = 0; i < sdi.fieldnrs.length; i++ ) {
        keyFields[ i ] = row[ sdi.fieldnrs[ i ] ];
      }
    }
    hash = calculateHashCode( keyFields );

    this.storeValues = sdi.storeValues;
    if ( storeValues ) {
      this.storedFieldValues = keyFields;
    }
  }

  private int calculateHashCode( Object[] keyFields ) {
    // deep used because Binary type is a native byte[]
    return Arrays.deepHashCode( keyFields );
  }

  @Override
  public boolean equals( Object obj ) {
    if ( storeValues ) {
      // deep used because Binary type is a native byte[]
      return Arrays.deepEquals( storedFieldValues, ( (RowKey) obj ).storedFieldValues );
    } else {
      return true;
    }
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
