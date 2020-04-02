/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.pipeline.transforms.pgpencryptstream;

import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.job.entries.pgpencryptfiles.GPG;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.TransformDataInterface;

/**
 * @author Samatar
 * @since 03-Juin-2008
 */
public class PGPEncryptStreamData extends BaseTransformData implements TransformDataInterface {
  public int indexOfField;
  public RowMetaInterface previousRowMeta;
  public RowMetaInterface outputRowMeta;
  public int NrPrevFields;

  public GPG gpg;
  public String keyName;
  public int indexOfKeyName;

  public PGPEncryptStreamData() {
    super();
    this.indexOfField = -1;
    this.gpg = null;
    this.keyName = null;
    this.indexOfKeyName = -1;
  }

}
