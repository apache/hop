/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.blockingtransform;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class BlockingTransformData extends BaseTransformData implements ITransformData {
  public List<FileObject> files;
  public List<Object[]> buffer;
  public List<InputStream> fis;
  public List<GZIPInputStream> gzis;
  public List<DataInputStream> dis;
  public List<Object[]> rowbuffer;

  public IRowMeta outputRowMeta;

  public int[] fieldnrs; // the corresponding field numbers;
  public FileObject fil;

  public BlockingTransformData() {
    super();

    buffer = new ArrayList<Object[]>( BlockingTransformMeta.CACHE_SIZE );
    files = new ArrayList<FileObject>();
    fis = new ArrayList<InputStream>();
    dis = new ArrayList<DataInputStream>();
    gzis = new ArrayList<GZIPInputStream>();
    rowbuffer = new ArrayList<Object[]>();
  }
}
