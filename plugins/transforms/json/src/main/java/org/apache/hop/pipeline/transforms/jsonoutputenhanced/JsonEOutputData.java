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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class JsonEOutputData extends BaseTransformData implements ITransformData {

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;
  public int inputRowMetaSize;
  public boolean rowsAreSafe;

  public int nrFields;
  public int[] fieldIndexes;
  public int[] keysGroupIndexes;
  public int nrRow;
  public List<ObjectNode> jsonItems;
  public List<ObjectNode> jsonKeyGroupItems;

  public String realBlocName;
  public int splitnr;
  public Writer writer;
  public CountingOutputStream countingStream;

  /** VFS path of the file currently open for write (lineage). */
  public String openedFilename;

  public boolean isWriteToFile;
  public String jsonSerialized;
  public long jsonLength;
  public Set<Integer> keyFields;

  /** */
  public JsonEOutputData() {
    super();

    this.nrRow = 0;
    this.writer = null;
    this.jsonKeyGroupItems = new ArrayList<>();
  }
}
