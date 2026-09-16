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

package org.apache.hop.spark.core;

import java.util.List;
import java.util.function.Consumer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.spark.sql.Row;

/**
 * Row handler for the plain generic case (one main input, no info or target streams): the partition
 * loop hands the current input row to the transform through a one-slot buffer and takes what the
 * transform writes straight into the Spark output queue, already converted.
 *
 * <p>This keeps the per-row protocol of the row-set based path — {@code processRow()} is called
 * once per input row and once more with an empty slot at the end so {@code getRow()} returns null —
 * but skips the Injector transform, the two row sets and the executor bookkeeping in between.
 */
public class SparkDirectRowHandler extends SparkRowHandler {

  private final BaseTransform transform;
  private final IRowMeta inputRowMeta;
  private final IRowMeta outputRowMeta;
  private final HopSparkRowConverter.RowCodec outputCodec;
  private final Consumer<Row> output;
  private Object[] slot;
  private boolean inputRowMetaSet;

  public SparkDirectRowHandler(
      BaseTransform transform,
      IRowMeta inputRowMeta,
      IRowMeta outputRowMeta,
      Consumer<Row> output) {
    super(transform);
    this.transform = transform;
    this.inputRowMeta = inputRowMeta;
    this.outputRowMeta = outputRowMeta;
    this.outputCodec = HopSparkRowConverter.RowCodec.of(outputRowMeta);
    this.output = output;
  }

  /** Make {@code row} the next row {@link #getRow()} returns; null means end of input. */
  public void offer(Object[] row) {
    this.slot = row;
  }

  @Override
  public Object[] getRow() throws HopException {
    Object[] row = slot;
    slot = null;
    if (!inputRowMetaSet) {
      transform.setInputRowMeta(inputRowMeta);
      inputRowMetaSet = true;
    }
    if (row != null) {
      transform.incrementLinesRead();
      List<IRowListener> rowListeners = transform.getRowListeners();
      if (!rowListeners.isEmpty()) {
        for (IRowListener rowListener : rowListeners) {
          rowListener.rowReadEvent(inputRowMeta, row);
        }
      }
    }
    return row;
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    List<IRowListener> rowListeners = transform.getRowListeners();
    if (!rowListeners.isEmpty()) {
      for (IRowListener rowListener : rowListeners) {
        rowListener.rowWrittenEvent(rowMeta, row);
      }
    }
    try {
      output.accept(outputCodec.toSpark(row));
    } catch (HopException e) {
      throw new HopTransformException(e);
    }
    transform.incrementLinesWritten();
  }
}
