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

package org.apache.hop.pipeline.transforms.sort;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class SortRowsData extends BaseTransformData implements ITransformData {
  public List<FileObject> files;
  public List<Object[]> buffer;
  public int getBufferIndex;

  public List<InputStream> fis;
  public List<GZIPInputStream> gzis;
  public List<DataInputStream> dis;
  public List<Object[]> rowbuffer;
  public List<Integer> bufferSizes;

  // To store rows and file references
  public List<RowTempFile> tempRows;

  public int[] fieldnrs; // the corresponding field numbers;
  public FileObject fil;
  public IRowMeta outputRowMeta;
  public int sortSize;
  public boolean compressFiles;
  public int[] convertKeysToNative;
  public boolean convertAnyKeysToNative;

  Comparator<RowTempFile> comparator;
  Comparator<Object[]> rowComparator;

  public int freeCounter;
  public int freeMemoryPct;
  public int minSortSize;
  public int freeMemoryPctLimit;
  public int memoryReporting;

  /*
   * Group Fields Implementation heroic
   */
  public Object[] previous;
  public int[] groupnrs;
  public boolean newBatch;

  public SortRowsData() {
    super();

    files = new ArrayList<>();
    fis = new ArrayList<>();
    gzis = new ArrayList<>();
    dis = new ArrayList<>();
    bufferSizes = new ArrayList<>();

    previous = null; // Heroic
  }

}
