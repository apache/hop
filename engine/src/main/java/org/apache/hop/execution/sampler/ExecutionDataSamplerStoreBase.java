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
 *
 */

package org.apache.hop.execution.sampler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.stream.IStream;

public abstract class ExecutionDataSamplerStoreBase<Store extends IExecutionDataSamplerStore>
    implements IExecutionDataSamplerStore {
  protected ExecutionDataSamplerMeta samplerMeta;
  protected IRowMeta rowMeta;
  protected List<Object[]> rows;
  protected int maxRows;

  public abstract Store getStore();

  public ExecutionDataSamplerStoreBase(
      ExecutionDataSamplerMeta samplerMeta, IRowMeta rowMeta, List<Object[]> rows, int maxRows) {
    this.samplerMeta = samplerMeta;
    this.rowMeta = rowMeta;
    this.rows = rows;
    this.maxRows = maxRows;
  }

  @Override
  public void init(IVariables variables, IRowMeta inputRowMeta, IRowMeta outputRowMeta) {
    rows = Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public IRowListener createRowListener(IExecutionDataSampler sampler) {
    return new IRowListener() {
      @Override
      public void rowReadEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
        try {
          sampler.sampleRow(getStore(), IStream.StreamType.INPUT, rowMeta, row);
        } catch (HopException e) {
          throw new HopTransformException("Error sampling read row", e);
        }
      }

      @Override
      public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
        try {
          sampler.sampleRow(getStore(), IStream.StreamType.OUTPUT, rowMeta, row);
        } catch (HopException e) {
          throw new HopTransformException("Error sampling written row", e);
        }
      }

      @Override
      public void errorRowWrittenEvent(IRowMeta rowMeta, Object[] row)
          throws HopTransformException {
        try {
          sampler.sampleRow(getStore(), IStream.StreamType.ERROR, rowMeta, row);
        } catch (HopException e) {
          throw new HopTransformException("Error sampling error row", e);
        }
      }
    };
  }

  public String getKeyForStore(String prefix, ExecutionDataSamplerMeta meta) {
    return prefix + "/" + meta.toString();
  }

  /**
   * Gets samplerMeta
   *
   * @return value of samplerMeta
   */
  public ExecutionDataSamplerMeta getSamplerMeta() {
    return samplerMeta;
  }

  /**
   * Sets samplerMeta
   *
   * @param samplerMeta value of samplerMeta
   */
  public void setSamplerMeta(ExecutionDataSamplerMeta samplerMeta) {
    this.samplerMeta = samplerMeta;
  }

  /**
   * Gets rowMeta
   *
   * @return value of rowMeta
   */
  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  /**
   * Sets rowMeta
   *
   * @param rowMeta value of rowMeta
   */
  public void setRowMeta(IRowMeta rowMeta) {
    this.rowMeta = rowMeta;
  }

  /**
   * Gets rows
   *
   * @return value of rows
   */
  public List<Object[]> getRows() {
    return rows;
  }

  /**
   * Sets rows
   *
   * @param rows value of rows
   */
  public void setRows(List<Object[]> rows) {
    this.rows = rows;
  }

  /**
   * Gets maxRows
   *
   * @return value of maxRows
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Sets maxRows
   *
   * @param maxRows value of maxRows
   */
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }
}
