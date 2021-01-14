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

package org.apache.hop.pipeline.transforms.yamlinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author Samatar
 * @since 21-06-2007
 */
public class YamlInputData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;

  public int nrInputFields;
  public Object[] readrow;
  public int totalPreviousFields;
  public int totalOutFields;
  public int totalOutStreamFields;

  /**
   * The YAML files to read
   */
  public FileInputList files;
  public FileObject file;
  public int filenr;

  public long rownr;
  public int indexOfYamlField;

  public YamlReader yaml;

  public IRowMeta rowMeta;

  public YamlInputData() {
    super();

    this.filenr = 0;
    this.indexOfYamlField = -1;
    this.nrInputFields = -1;
    this.readrow = null;
    this.totalPreviousFields = 0;
    this.file = null;
    this.totalOutFields = 0;
    this.totalOutStreamFields = 0;
  }
}
