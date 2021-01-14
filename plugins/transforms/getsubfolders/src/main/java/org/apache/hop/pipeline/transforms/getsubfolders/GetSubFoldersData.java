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

package org.apache.hop.pipeline.transforms.getsubfolders;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author Samatar
 * @since 18-July-2008
 */
public class GetSubFoldersData extends BaseTransformData implements ITransformData {

  public Object[] previousRow;

  public IRowMeta outputRowMeta;

  public FileInputList files;

  public boolean isLastFile;

  public int filenr;

  public int filessize;

  public FileObject file;

  public long rownr;

  public int totalpreviousfields;

  public int indexOfFoldernameField;

  public IRowMeta inputRowMeta;

  public Object[] readrow;

  public int nrTransformFields;

  public GetSubFoldersData() {
    super();
    previousRow = null;
    filenr = 0;
    filessize = 0;
    file = null;
    totalpreviousfields = 0;
    indexOfFoldernameField = -1;
    readrow = null;
    nrTransformFields = 0;
  }

}
