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

package org.apache.hop.pipeline.transforms.delete;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.sql.PreparedStatement;

/**
 * @author Tom
 * @since 28-March-2006
 */
public class DeleteData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...
  public int[] keynrs2; // nr of keylookup2-value in row...

  public IRowMeta outputRowMeta;

  public String schemaTable;

  public IRowMeta deleteParameterRowMeta;

  public PreparedStatement prepStatementDelete;

  public DeleteData() {
    super();

    db = null;
  }

}
