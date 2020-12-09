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

package org.apache.hop.pipeline.transforms.fuzzymatch;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.HashSet;

/**
 * @author Samatar
 * @since 24-jan-2010
 */
public class FuzzyMatchData extends BaseTransformData implements ITransformData {
  public IRowMeta previousRowMeta;
  public IRowMeta outputRowMeta;

  /**
   * used to store values in used to look up things
   */
  public HashSet<Object[]> look;

  public boolean readLookupValues;

  /**
   * index of main stream field
   **/
  public int indexOfMainField;

  public int minimalDistance;

  public int maximalDistance;

  public double minimalSimilarity;

  public double maximalSimilarity;

  public String valueSeparator;

  public IRowMeta infoMeta;

  public IStream infoStream;

  public boolean addValueFieldName;
  public boolean addAdditionalFields;

  /**
   * index of return fields from lookup stream
   **/
  public int[] indexOfCachedFields;
  public int nrCachedFields;
  public IRowMeta infoCache;

  public FuzzyMatchData() {
    super();
    this.look = new HashSet<>();
    this.indexOfMainField = -1;
    this.addValueFieldName = false;
    this.valueSeparator = "";
    this.nrCachedFields = 1;
    this.addAdditionalFields = false;
  }

}
