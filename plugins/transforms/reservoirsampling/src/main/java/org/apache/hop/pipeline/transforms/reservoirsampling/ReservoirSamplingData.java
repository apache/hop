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

package org.apache.hop.pipeline.transforms.reservoirsampling;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * Holds temporary data (i.e. sampled rows). Implements the reservoir sampling algorithm "R" by
 * Jeffrey Scott Vitter.
 *
 * <p>For more information see:<br>
 * <br>
 *
 * <p>Vitter, J. S. Random Sampling with a Reservoir. ACM Transactions on Mathematical Software,
 * Vol. 11, No. 1, March 1985. Pages 37-57.
 *
 * @version 1.0
 */
@SuppressWarnings("java:S1104")
public class ReservoirSamplingData extends BaseTransformData implements ITransformData {

  // the output data format
  protected IRowMeta outputRowMeta;

  // holds the sampled rows
  protected List<Object[]> samples = null;

  // the size of the sample
  protected int sampleSize;

  // the current row number
  protected int currentInputRow;

  // random number generator
  protected Random random;

  // state of processing
  protected PROC_MODE state;

  public enum PROC_MODE {
    SAMPLING,
    PASS_THROUGH,
    DISABLED
  }

  public ReservoirSamplingData() {
    super();
  }

  /**
   * Set the meta data for the output format
   *
   * @param rmi a <code>IRowMeta</code> value
   */
  public void setOutputRowMeta(IRowMeta rmi) {
    outputRowMeta = rmi;
  }

  /**
   * Get the output meta data
   *
   * @return a <code>IRowMeta</code> value
   */
  public IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  /**
   * Gets the sample as an array of rows
   *
   * @return the sampled rows
   */
  public List<Object[]> getSamples() {
    return samples;
  }

  /**
   * Initialize this data object
   *
   * @param sampleSize the number of rows to sample
   * @param seed the seed for the random number generator
   */
  public void initialize(int sampleSize, int seed) {
    this.sampleSize = sampleSize;

    if (this.sampleSize == 0) {
      state = PROC_MODE.PASS_THROUGH;
    } else if (this.sampleSize < 0) {
      state = PROC_MODE.DISABLED;
    } else {
      state = PROC_MODE.SAMPLING;
    }

    samples = (this.sampleSize > 0) ? new ArrayList<>(this.sampleSize) : new ArrayList<>();
    currentInputRow = 0;
    random = new Random(seed);

    // throw away the first 100 random numbers
    for (int i = 0; i < 100; i++) {
      random.nextDouble();
    }
  }

  /**
   * Determine the current operational state of the Reservoir Sampling transform. Sampling,
   * PassThrough(Do not wait until end, pass through on the fly), Disabled.
   *
   * @return current operational state
   */
  public PROC_MODE getProcessingMode() {
    return state;
  }

  /**
   * Set this component to sample, pass through or be disabled
   *
   * @param state member of PROC_MODE enumeration indicating the desired operational state
   */
  public void setProcessingMode(PROC_MODE state) {
    this.state = state;
  }

  /**
   * Here is where the action happens. Sampling is done using the "R" algorithm of Jeffrey Scott
   * Vitter.
   *
   * @param row an incoming row
   */
  public void addRowToSamples(Object[] row) {
    if (currentInputRow < sampleSize) {
      // Fill sample size with first available data
      setElement(samples, currentInputRow, row);
    } else if (sampleSize > 0) {
      // Replace random positions within the sample
      double r = random.nextDouble();
      if (r < ((double) sampleSize / (double) currentInputRow)) {
        r = random.nextDouble();
        int replace = (int) (sampleSize * r);
        setElement(samples, replace, row);
      }
    }
    currentInputRow++;
  }

  // brute force way of filling list when item index is out of range,
  // should be ported to a commons or some library call or something
  // that works well with the "R" randomizing algorithm
  private void setElement(List<Object[]> list, int idx, Object item) {
    final int size = list.size();
    if (size <= idx) {
      int buff = (size == 0) ? 100 : size * 2;
      for (int i = 0; i < buff; i++) {
        list.add(null);
      }
    }
    list.set(idx, (Object[]) item);
  }

  public void cleanUp() {
    samples = null;
  }
}
