/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.tika;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.Date;

/**
 * @author MBurgess
 * @since 02-Jul-2013
 */
public class TikaData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;
  public IRowMeta convertRowMeta;
  public Object[] previousRow;
  public int nr_repeats;

  public FileInputList files;
  public boolean last_file;
  public FileObject file;
  public int fileNr;

  public long rowNr;
  public int indexOfFilenameField;
  public int totalPreviousFields;

  public Object[] readRow;

  public String fileContent;

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

  public TikaOutput tikaOutput;

  /** */
  public TikaData() {
    super();

    nr_repeats = 0;
    previousRow = null;
    fileNr = 0;

    totalPreviousFields = 0;
    indexOfFilenameField = -1;

    readRow = null;
    fileSize = 0;
  }
}
