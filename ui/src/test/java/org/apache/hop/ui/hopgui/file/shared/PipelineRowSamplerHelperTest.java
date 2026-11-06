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

package org.apache.hop.ui.hopgui.file.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration.SampleType;
import org.apache.hop.ui.hopgui.file.shared.PipelineRowSamplerHelper.RowSampler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PipelineRowSamplerHelperTest {

  private static final int SAMPLE_SIZE = 10;

  private IRowMeta rowMeta;

  @BeforeEach
  void setUp() {
    rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("nr"));
    rowMeta.addValueMeta(new ValueMetaDate("date"));
  }

  private static Object[] row(long nr) {
    return new Object[] {nr, new Date(nr)};
  }

  private static List<Long> numbers(RowSampler sampler) {
    List<Object[]> rows = sampler.getRowBuffer().getBuffer();
    List<Long> numbers = new ArrayList<>();
    synchronized (rows) {
      for (Object[] r : rows) {
        numbers.add((Long) r[0]);
      }
    }
    return numbers;
  }

  private RowSampler feed(SampleType sampleType, int rowCount) throws Exception {
    RowSampler sampler = PipelineRowSamplerHelper.createSampler(sampleType, SAMPLE_SIZE, rowMeta);
    for (long nr = 1; nr <= rowCount; nr++) {
      sampler.sample(rowMeta, row(nr));
    }
    return sampler;
  }

  @Test
  void firstKeepsTheFirstRows() throws Exception {
    assertEquals(List.of(1L, 2L, 3L), numbers(feed(SampleType.First, 3)));
    assertEquals(
        List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), numbers(feed(SampleType.First, 1000)));
  }

  @Test
  void lastKeepsTheLastRowsNewestFirst() throws Exception {
    assertEquals(List.of(3L, 2L, 1L), numbers(feed(SampleType.Last, 3)));
    assertEquals(
        List.of(1000L, 999L, 998L, 997L, 996L, 995L, 994L, 993L, 992L, 991L),
        numbers(feed(SampleType.Last, 1000)));
  }

  @Test
  void lastAtExactlyTheSampleSize() throws Exception {
    assertEquals(
        List.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L),
        numbers(feed(SampleType.Last, SAMPLE_SIZE)));
    assertEquals(
        List.of(11L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L),
        numbers(feed(SampleType.Last, SAMPLE_SIZE + 1)));
  }

  @Test
  void randomKeepsAllRowsWhileThereAreFewerThanTheSampleSize() throws Exception {
    assertEquals(List.of(1L, 2L, 3L), numbers(feed(SampleType.Random, 3)));
  }

  @Test
  void randomKeepsSampleSizeDistinctInputRows() throws Exception {
    List<Long> numbers = numbers(feed(SampleType.Random, 10_000));
    assertEquals(SAMPLE_SIZE, numbers.size());
    Set<Long> distinct = new HashSet<>(numbers);
    assertEquals(SAMPLE_SIZE, distinct.size());
    for (long nr : numbers) {
      assertTrue(nr >= 1 && nr <= 10_000, "row " + nr + " was never written");
    }
    // With 10,000 rows, a sample made only of the first rows means the reservoir never replaced
    assertTrue(numbers.stream().anyMatch(nr -> nr > SAMPLE_SIZE));
  }

  @Test
  void keptRowsAreCopies() throws Exception {
    for (SampleType sampleType : List.of(SampleType.First, SampleType.Last, SampleType.Random)) {
      RowSampler sampler = PipelineRowSamplerHelper.createSampler(sampleType, SAMPLE_SIZE, rowMeta);
      Object[] written = row(42);
      sampler.sample(rowMeta, written);

      // A later transform may replace values in the array it received, as Value Mapper does
      written[0] = 0L;
      written[1] = new Date(0L);

      Object[] kept = sampler.getRowBuffer().getBuffer().get(0);
      assertNotSame(written, kept, sampleType.name());
      assertEquals(42L, kept[0], sampleType.name());
      assertEquals(42L, ((Date) kept[1]).getTime(), sampleType.name());
    }
  }
}
