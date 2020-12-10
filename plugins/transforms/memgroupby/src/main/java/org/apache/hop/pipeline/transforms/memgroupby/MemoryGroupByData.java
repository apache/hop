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

package org.apache.hop.pipeline.transforms.memgroupby;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.HashMap;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class MemoryGroupByData extends BaseTransformData implements ITransformData {
  public class HashEntry {
    private Object[] groupData;

    public HashEntry( Object[] groupData ) {
      this.groupData = groupData;
    }

    public Object[] getGroupData() {
      return groupData;
    }

    public boolean equals( Object obj ) {
      HashEntry entry = (HashEntry) obj;

      try {
        return groupMeta.compare( groupData, entry.groupData ) == 0;
      } catch ( HopValueException e ) {
        throw new RuntimeException( e );
      }
    }

    public int hashCode() {
      try {
        return groupMeta.hashCode( getHashValue() );
      } catch ( HopValueException e ) {
        throw new RuntimeException( e );
      }
    }

    private Object[] getHashValue() throws HopValueException {
      Object[] groupDataHash = new Object[ groupMeta.size() ];
      for ( int i = 0; i < groupMeta.size(); i++ ) {
        IValueMeta valueMeta = groupMeta.getValueMeta( i );
        groupDataHash[ i ] = valueMeta.convertToNormalStorageType( groupData[ i ] );
      }
      return groupDataHash;
    }
  }

  public HashMap<HashEntry, Aggregate> map;

  public IRowMeta aggMeta;
  public IRowMeta groupMeta;
  public IRowMeta entryMeta;

  public IRowMeta groupAggMeta; // for speed: groupMeta+aggMeta
  public int[] groupnrs;
  public int[] subjectnrs;

  public boolean firstRead;

  public Object[] groupResult;

  public boolean hasOutput;

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;

  public IValueMeta valueMetaInteger;
  public IValueMeta valueMetaNumber;

  public boolean newBatch;

  public MemoryGroupByData() {
    super();

  }

  public HashEntry getHashEntry( Object[] groupData ) {
    return new HashEntry( groupData );
  }

  /**
   * Method responsible for clearing out memory hogs
   */
  public void clear() {
    map = new HashMap<>();
  }
}
