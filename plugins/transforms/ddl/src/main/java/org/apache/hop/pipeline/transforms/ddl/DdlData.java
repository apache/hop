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

package org.apache.hop.pipeline.transforms.ddl;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/** Data class for the Ddl transform. */
public class DdlData extends BaseTransformData implements ITransformData {
  public String connectionName;
  public DatabaseMeta databaseMeta;
  public String schemaName;
  public String tableName;
  public String nameField;
  public String typeField;
  public String lengthField;
  public String precisionField;
  public IRowMeta ddlRowMeta;
  public String ddlOutputField;

  /** Default constructor for DdlData. */
  public DdlData() {
    // Not used
  }
}
