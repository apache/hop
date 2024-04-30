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

package org.apache.hop.pipeline.transforms.getfilesrowcount;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class GetFilesRowsCountData extends BaseTransformData implements ITransformData {
  public String thisLine;
  public IRowMeta outputRowMeta;
  public IRowMeta convertRowMeta;
  public Object[] previousRow;

  public FileInputList files;
  public boolean lastFile;
  public FileObject file;
  public long fileNumber;

  public long rowNumber;
  public int fileFormatType;
  public StringBuilder lineStringBuilder;
  public int totalPreviousFields;
  public int indexOfFilenameField;
  public Object[] inputRow;
  public IRowMeta inputRowMeta;
  public char separator;

  public boolean foundData;

  /** */
  public GetFilesRowsCountData() {
    super();
    previousRow = null;
    thisLine = null;

    lineStringBuilder = new StringBuilder(256);
    totalPreviousFields = 0;
    indexOfFilenameField = -1;
    inputRow = null;
    separator = '\n';
    foundData = false;
  }
}
