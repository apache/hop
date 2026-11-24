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

package org.apache.hop.beam.core.transform;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.hop.beam.core.HopRow;

/**
 * Collects every row of a bounded collection in one list, for {@link TransformWholeStreamFn}. As a
 * {@code Combine.globally} it gives exactly one list, also an empty one when there are no rows: a
 * transform such as Detect empty stream needs to run then too.
 */
public class AllRowsFn extends Combine.CombineFn<HopRow, List<HopRow>, List<HopRow>> {
  @Serial private static final long serialVersionUID = 1L;

  @Override
  public List<HopRow> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<HopRow> addInput(List<HopRow> rows, HopRow row) {
    rows.add(row);
    return rows;
  }

  @Override
  public List<HopRow> mergeAccumulators(Iterable<List<HopRow>> accumulators) {
    List<HopRow> all = new ArrayList<>();
    for (List<HopRow> rows : accumulators) {
      all.addAll(rows);
    }
    return all;
  }

  @Override
  public List<HopRow> extractOutput(List<HopRow> rows) {
    return rows;
  }

  @Override
  public Coder<List<HopRow>> getAccumulatorCoder(CoderRegistry registry, Coder<HopRow> inputCoder) {
    return ListCoder.of(inputCoder);
  }

  @Override
  public Coder<List<HopRow>> getDefaultOutputCoder(
      CoderRegistry registry, Coder<HopRow> inputCoder) {
    return ListCoder.of(inputCoder);
  }
}
