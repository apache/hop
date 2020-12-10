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

package org.apache.hop.pipeline.transforms.streamlookup;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.hash.ByteArrayHashIndex;
import org.apache.hop.core.hash.LongHashIndex;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class StreamLookupData extends BaseTransformData implements ITransformData {
  /**
   * used to store values in used to look up things
   */
  public Map<RowMetaAndData, Object[]> look;

  public List<KeyValue> list;

  /**
   * nrs of keys-values in row.
   */
  public int[] keynrs;

  /**
   * The metadata we send out
   */
  public IRowMeta outputRowMeta;

  /**
   * default string converted to values...
   */
  public Object[] nullIf;

  /**
   * Flag to indicate that we have to read lookup values from the info transform
   */
  public boolean readLookupValues;

  /**
   * Stores the first row of the lookup-values to later determine if the types are the same as the input row lookup
   * values.
   */
  public IRowMeta keyTypes;

  public IRowMeta cacheKeyMeta;

  public IRowMeta cacheValueMeta;

  public Comparator<KeyValue> comparator;

  public ByteArrayHashIndex hashIndex;
  public LongHashIndex longIndex;

  public IRowMeta lookupMeta;

  public IRowMeta infoMeta;

  public int[] lookupColumnIndex;

  public boolean metadataVerifiedIntegerPair;

  /**
   * See if we need to convert the keys to a native data type
   */
  public boolean[] convertKeysToNative;

  // Did we read rows from the lookup hop.
  public boolean hasLookupRows;

  public IStream infoStream;

  public StreamLookupData() {
    super();
    look = new HashMap<>();
    hashIndex = null;
    longIndex = new LongHashIndex();
    list = new ArrayList<>();
    metadataVerifiedIntegerPair = false;
    hasLookupRows = false;

    comparator = ( k1, k2 ) -> {
      try {
        return cacheKeyMeta.compare( k1.getKey(), k2.getKey() );
      } catch ( HopValueException e ) {
        throw new RuntimeException( "Stream Lookup comparator error", e );
      }
    };
  }

}
