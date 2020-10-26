/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.zipfile;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author Samatar
 * @since 03-Juin-2008
 */
public class ZipFileData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;

  public int indexOfSourceFilename;
  public int indexOfZipFilename;
  public FileObject sourceFile;
  public FileObject zipFile;

  public int indexOfBaseFolder;
  public String baseFolder;

  public int indexOfMoveToFolder;

  public ZipFileData() {
    super();
    indexOfSourceFilename = -1;
    indexOfZipFilename = -1;
    sourceFile = null;
    zipFile = null;
    indexOfBaseFolder = -1;
    baseFolder = null;
    indexOfMoveToFolder = -1;
  }

}
