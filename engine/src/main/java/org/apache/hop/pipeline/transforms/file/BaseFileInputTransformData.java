/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2017-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.file;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Some base data for file-based input transforms.
 *
 * @author Alexander Buloichik
 */
public class BaseFileInputTransformData extends BaseTransformData {

  public IFileErrorHandler dataErrorLineHandler;

  /**
   * Files for process.
   */
  public FileInputList files;

  /**
   * Current file info.
   */
  public String filename;
  public int currentFileIndex;
  public FileObject file;

  /**
   * Reader for current file.
   */
  public IBaseFileInputReader reader;

  public IRowMeta outputRowMeta;

  public HashMap<String, Object[]> passThruFields;

  public Object[] currentPassThruFieldsRow;

  public int nrPassThruFields;

  public IRowMeta convertRowMeta;

  public int nr_repeats;
  // public boolean isLastFile;

  public Map<String, Boolean> rejectedFiles = new HashMap<String, Boolean>();

  /**
   * File-dependent data for fill additional fields.
   */
  public String shortFilename;
  public String path;
  public String extension;
  public boolean hidden;
  public Date lastModificationDateTime;
  public String uriName;
  public String rootUriName;
  public Long size;
}
