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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Transform that will abort after having seen 'x' number of rows on its input. */
public class Ddl extends BaseTransform<DdlMeta, DdlData> {
  public Ddl(
      TransformMeta transformMeta,
      DdlMeta meta,
      DdlData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    // Add init code here.
    //
    data.connectionName = resolve(meta.getConnectionName());
    if (StringUtils.isEmpty(data.connectionName)) {
      logError("There is no database connection name specified.");
      setErrors(1L);
      return false;
    }
    try {
      data.databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(data.connectionName);
    } catch (HopException e) {
      logError("Error loading database connection " + data.connectionName, e);
      setErrors(1L);
      return false;
    }
    data.schemaName = resolve(meta.getSchemaName());
    data.tableName = resolve(meta.getTableName());
    data.nameField = resolve(meta.getFieldNameField());
    data.typeField = resolve(meta.getFieldTypeField());
    data.lengthField = resolve(meta.getFieldLengthField());
    data.precisionField = resolve(meta.getFieldPrecisionField());
    data.ddlOutputField = resolve(meta.getDdlOutputField());
    data.ddlRowMeta = new RowMeta();
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    // Get row from input rowset & set row busy!
    Object[] r = getRow();

    // no more input to be expected...
    if (r == null) {
      // This is where we have the complete row metadata for the target table.
      //
      executeDdl();

      setOutputDone();
      return false;
    }

    String name = getInputRowMeta().getString(r, data.nameField, null);
    if (StringUtils.isEmpty(name)) {
      throw new HopException(
          "The field name in '" + data.nameField + "' of the input row is empty");
    }

    String type = getInputRowMeta().getString(r, data.typeField, null);
    if (StringUtils.isEmpty(type)) {
      throw new HopException(
          "The field type in '" + data.typeField + "' of the input row is not specified");
    }
    int hopType = ValueMetaFactory.getIdForValueMeta(type);

    String lengthString = getInputRowMeta().getString(r, data.lengthField, null);
    int length = Const.toInt(lengthString, -1);
    String precisionString = getInputRowMeta().getString(r, data.precisionField, null);
    int precision = Const.toInt(precisionString, -1);

    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, hopType, length, precision);
    data.ddlRowMeta.addValueMeta(valueMeta);

    return true;
  }

  private void executeDdl() throws HopException {
    try {
      try (Database database = new Database(this, this, data.databaseMeta)) {
        database.connect();

        String schemaTable =
            data.databaseMeta.getQuotedSchemaTableCombination(
                this, data.schemaName, data.tableName);

        // We'll only drop the table if we're executing DDL, just to avoid confusion.
        //
        if (meta.isExecutingDdl() && meta.isDroppingTable()) {
          boolean tableExists = database.checkTableExists(data.schemaName, data.tableName);
          if (tableExists) {
            database.execStatement("DROP TABLE " + schemaTable);
            logBasic("Table " + schemaTable + " was dropped.");
          }
        }

        String ddl = database.getDDL(schemaTable, data.ddlRowMeta);
        if (StringUtils.isEmpty(ddl)) {
          // We're done here.  The table looks fine as it is.
          //
          logDetailed("There was nothing to execute for table '" + schemaTable + "'.");
          return;
        }

        // Do we need to execute the DDL?
        //
        if (meta.isExecutingDdl()) {
          database.execStatements(ddl);
          incrementLinesOutput();
        }

        // Output the generated DDL?
        //
        if (StringUtils.isNotEmpty(data.ddlOutputField)) {
          IRowMeta outputRowMeta = getInputRowMeta().clone();
          meta.getFields(outputRowMeta, getTransformName(), null, null, this, metadataProvider);
          Object[] outputRow = RowDataUtil.allocateRowData(outputRowMeta.size());
          outputRow[0] = ddl;
          putRow(outputRowMeta, outputRow);
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error executing DDL for row metadata: " + data.ddlRowMeta.toStringMeta(), e);
    }
  }
}
