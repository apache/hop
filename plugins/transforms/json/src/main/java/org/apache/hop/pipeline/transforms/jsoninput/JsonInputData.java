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

package org.apache.hop.pipeline.transforms.jsoninput;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransformData;
import org.apache.hop.pipeline.transforms.jsoninput.reader.IJsonReader;

import java.io.InputStream;
import java.util.BitSet;
import java.util.Iterator;

/**
 * @author Samatar
 * @since 21-06-2010
 */
public class JsonInputData extends BaseFileInputTransformData implements ITransformData {
  public Object[] previousRow;
  public IRowMeta inputRowMeta;

  public boolean hasFirstRow;

  public int nrInputFields;

  /**
   * last row read
   */
  public Object[] readrow;
  public int totalpreviousfields;

  public int filenr;

  /**
   * output row counter
   */
  public long rownr;
  public int indexSourceField;

  public Iterator<InputStream> inputs;
  public IJsonReader reader;
  public IRowSet readerRowSet;
  public BitSet repeatedFields;

  public JsonInputData() {
    super();
    nr_repeats = 0;
    previousRow = null;
    filenr = 0;

    indexSourceField = -1;

    nrInputFields = -1;

    readrow = null;
    totalpreviousfields = 0;
  }

}
