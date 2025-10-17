/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.addsnowflakeid;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Adds snowflake ID is a 64-bit globally unique identifier to a stream of rows
 *
 * @author lance
 * @since 2025/10/16 21:19
 */
public class AddSnowflakeId extends BaseTransform<AddSnowflakeIdMeta, AddSnowflakeIdData> {
  private static final Class<?> PKG = AddSnowflakeId.class;
  private SnowflakeSafeIdGenerator generator;

  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this
   * class to implement your own transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta AddSnowflakeIdMeta
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hashtables etc.
   * @param copyNr The copynumber for this transform.
   * @param pipelineMeta The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transforms.
   */
  public AddSnowflakeId(
      TransformMeta transformMeta,
      AddSnowflakeIdMeta meta,
      AddSnowflakeIdData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (super.init()) {
      if (isBasic()) {
        logBasic(
            "snowflake dataCenter: {0},machine: {1}", meta.getDataCenterId(), meta.getMachineId());
      }

      generator = new SnowflakeSafeIdGenerator(meta.getDataCenterId(), meta.getMachineId());
      return true;
    }

    return false;
  }

  @Override
  public boolean processRow() throws HopException {
    // Get row from input rowSet & set row busy!
    Object[] r = getRow();
    if (r == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "AddSnowflakeId.Log.ReadRow")
              + getLinesRead()
              + " : "
              + getInputRowMeta().getString(r));
    }

    try {
      IRowMeta outputRowMeta = data.outputRowMeta;
      // resize array
      Object[] outputRowData = RowDataUtil.resizeArray(r, outputRowMeta.size());
      outputRowData[outputRowMeta.size() - 1] = generator.nextId();

      putRow(data.outputRowMeta, outputRowData);

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "AddSnowflakeId.Log.WriteRow")
                + getLinesWritten()
                + " : "
                + getInputRowMeta().getString(r));
      }

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "AddSnowflakeId.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "AddSnowflakeId.Log.ErrorInTransform") + e.getMessage());
      setErrors(1);
      stopAll();
      setOutputDone();
      return false;
    }

    return true;
  }
}
