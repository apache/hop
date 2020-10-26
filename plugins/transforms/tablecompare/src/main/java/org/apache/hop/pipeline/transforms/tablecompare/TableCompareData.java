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

package org.apache.hop.pipeline.transforms.tablecompare;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author Matt
 * @since 19-11-2009
 */
public class TableCompareData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public IRowMeta convertRowMeta;

  public int refSchemaIndex;
  public int refTableIndex;
  public int cmpSchemaIndex;
  public int cmpTableIndex;
  public int keyFieldsIndex;
  public int excludeFieldsIndex;

  public Database referenceDb;
  public Database compareDb;
  public IRowMeta errorRowMeta;

  public int keyDescIndex;
  public int valueReferenceIndex;
  public int valueCompareIndex;

  public TableCompareData() {
    super();
  }

}
