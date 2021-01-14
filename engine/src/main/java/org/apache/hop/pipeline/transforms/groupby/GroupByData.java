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

package org.apache.hop.pipeline.transforms.groupby;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class GroupByData extends BaseTransformData implements ITransformData {
  public Object[] previous;

  /**
   * target value meta for aggregation fields
   */
  public IRowMeta aggMeta;
  public Object[] agg;
  public IRowMeta groupMeta;
  public IRowMeta groupAggMeta; // for speed: groupMeta+aggMeta
  public int[] groupnrs;
  /**
   * array, length is equal to aggMeta value
   * meta list size and metadata subject fields length. Values corresponds to input
   * values used to calculate target results.
   */
  public int[] subjectnrs;
  public long[] counts;

  public Set<Object>[] distinctObjs;

  public ArrayList<Object[]> bufferList;

  public File tempFile;

  public FileOutputStream fosToTempFile;

  public DataOutputStream dosToTempFile;

  public int rowsOnFile;

  public boolean firstRead;

  public FileInputStream fisToTmpFile;
  public DataInputStream disToTmpFile;

  public Object[] groupResult;

  public boolean hasOutput;

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;

  public List<Integer> cumulativeSumSourceIndexes;
  public List<Integer> cumulativeSumTargetIndexes;

  public List<Integer> cumulativeAvgSourceIndexes;
  public List<Integer> cumulativeAvgTargetIndexes;

  public Object[] previousSums;

  public Object[] previousAvgSum;

  public long[] previousAvgCount;

  public IValueMeta valueMetaInteger;
  public IValueMeta valueMetaNumber;

  public double[] mean;

  public boolean newBatch;

  public GroupByData() {
    super();

    previous = null;
  }

}
