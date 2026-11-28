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
import java.util.function.Supplier;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;

/**
 * Row handler for the plain generic case (one main input, no info or target streams): {@code
 * getRow()} takes the next row of the Spark partition and {@code putRow()} puts what the transform
 * writes straight into the Spark output queue, already converted.
 *
 * <p>The transform pulls its input, as on the local engine: it may read several rows in one {@code
 * processRow()} (CSV input collects all file names first) and keep writing after the last one, and
 * {@code getRow()} returns null only at the real end of the partition. The partition loop calls
 * {@code processRow()} until it returns false. No Injector transform, row sets or executor
 * bookkeeping in between.
 */
public class SparkDirectRowHandler extends SparkRowHandler {

  private final BaseTransform transform;
  private final IRowMeta inputRowMeta;
  private final IRowMeta outputRowMeta;
  private final HopSparkRowConverter.RowCodec outputCodec;
  private final Consumer<Object[]> output;
  private final boolean handOff;
  private final Supplier<Object[]> input;
  private boolean inputRowMetaSet;

  public SparkDirectRowHandler(
      BaseTransform transform,
      IRowMeta inputRowMeta,
      IRowMeta outputRowMeta,
      Supplier<Object[]> input,
      Consumer<Object[]> output) {
    this(transform, inputRowMeta, outputRowMeta, input, output, false);
  }

  private SparkDirectRowHandler(
      BaseTransform transform,
      IRowMeta inputRowMeta,
      IRowMeta outputRowMeta,
      Supplier<Object[]> input,
      Consumer<Object[]> output,
      boolean handOff) {
    super(transform);
    this.input = input;
    this.transform = transform;
    this.inputRowMeta = inputRowMeta;
    this.outputRowMeta = outputRowMeta;
    this.outputCodec = HopSparkRowConverter.RowCodec.of(outputRowMeta);
    this.output = output;
    this.handOff = handOff;
  }

  /**
   * A handler for a transform inside a fused chain that is not the last one: what it writes goes to
   * the next transform of the chain as Hop rows, in the layout {@code outputRowMeta} declares (see
   * {@link HopSparkRowConverter.RowCodec#handOff}).
   */
  public static SparkDirectRowHandler toNextTransform(
      BaseTransform transform,
      IRowMeta inputRowMeta,
      IRowMeta outputRowMeta,
      Supplier<Object[]> input,
      Consumer<Object[]> next) {
    return new SparkDirectRowHandler(transform, inputRowMeta, outputRowMeta, input, next, true);
  }

  @Override
  public Object[] getRow() throws HopException {
    Object[] row = input.get();
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
      Object[] normal = HopSparkRowConverter.toNormalStorage(rowMeta, row);
      output.accept(handOff ? outputCodec.handOff(normal) : normal);
    } catch (HopException e) {
      throw new HopTransformException(e);
    }
    transform.incrementLinesWritten();
  }
}
