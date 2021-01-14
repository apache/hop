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

package org.apache.hop.pipeline.transforms.loadfileinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.Date;

/**
 * @author Samatar
 * @since 21-06-2007
 */
public class LoadFileInputData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;
  public IRowMeta convertRowMeta;
  public String thisline, nextline, lastline;
  public Object[] previousRow;
  public int nr_repeats;

  public FileInputList files;
  public boolean last_file;
  public FileObject file;
  public int filenr;

  public long rownr;
  public int indexOfFilenameField;
  public int totalpreviousfields;
  public int nrInputFields;

  public Object[] readrow;

  public byte[] filecontent;

  public long fileSize;

  public IRowMeta inputRowMeta;
  public String filename;
  public String shortFilename;
  public String path;
  public String extension;
  public boolean hidden;
  public Date lastModificationDateTime;
  public String uriName;
  public String rootUriName;

  public LoadFileInputData() {
    super();

    nr_repeats = 0;
    previousRow = null;
    filenr = 0;

    totalpreviousfields = 0;
    indexOfFilenameField = -1;
    nrInputFields = -1;

    readrow = null;
    fileSize = 0;

  }

}
