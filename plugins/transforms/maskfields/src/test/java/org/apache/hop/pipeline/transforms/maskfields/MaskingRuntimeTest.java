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

package org.apache.hop.pipeline.transforms.maskfields;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.UUID;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.maskfields.MaskingEngine.Binding;
import org.junit.jupiter.api.Test;

class MaskingRuntimeTest {

  @Test
  void memoryIsSharedInsideOneExecutionAndResetForTheNext() throws Exception {
    String executionId = uniqueId();
    MaskingPattern pattern = memoryPattern("First name", "fn", 1);
    MaskingRuntime runtime = MaskingRuntime.getInstance();

    MaskingRuntime.Lease first = runtime.acquire(executionId);
    MaskingRuntime.Lease second = runtime.acquire(executionId);
    try {
      MaskingEngine firstEngine = engine(first, "Mask", "name", pattern);
      MaskingEngine secondEngine = engine(second, "Mask", "name", pattern);
      RowMeta rowMeta = stringRow("name");
      assertEquals("fn1", apply(firstEngine, rowMeta, "Matt"));
      assertEquals("fn1", apply(secondEngine, rowMeta, "Matt"));
      assertEquals("fn2", apply(secondEngine, rowMeta, "Ann"));
    } finally {
      first.release();
      second.release();
    }

    MaskingRuntime.Lease nextRun = runtime.acquire(uniqueId());
    try {
      assertEquals(
          "fn1", apply(engine(nextRun, "Mask", "name", pattern), stringRow("name"), "Matt"));
    } finally {
      nextRun.release();
    }
  }

  @Test
  void unrememberedSequencesAreSharedPerField() throws Exception {
    String executionId = uniqueId();
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName("House");
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(MaskingToken.SEQUENCE);
    pattern.setStorage(MaskingStorage.NONE);
    pattern.setPrefix("hn");
    pattern.setSequenceStart("1");
    MaskingRuntime runtime = MaskingRuntime.getInstance();
    MaskingRuntime.Lease first = runtime.acquire(executionId);
    MaskingRuntime.Lease second = runtime.acquire(executionId);
    try {
      MaskingEngine houseA = engine(first, "Mask", "houseNr", pattern);
      MaskingEngine houseB = engine(second, "Mask", "houseNr", pattern);
      MaskingEngine cell = engine(first, "Mask", "telCell", pattern);
      RowMeta house = stringRow("houseNr");
      RowMeta phone = stringRow("telCell");
      assertEquals("hn1", apply(houseA, house, "1"));
      assertEquals("hn2", apply(houseB, house, "1"));
      assertEquals("hn1", apply(cell, phone, "1"));
    } finally {
      first.release();
      second.release();
    }
  }

  private static MaskingEngine engine(
      MaskingRuntime.Lease lease, String transformName, String fieldName, MaskingPattern pattern) {
    boolean remembered = pattern.remembers();
    long start = Long.parseLong(pattern.getSequenceStart());
    Binding binding =
        new Binding(
            fieldName,
            pattern,
            pattern.getPrefix(),
            pattern.getSuffix(),
            start,
            remembered ? lease.memory() : null,
            remembered ? null : lease.sequence(transformName, fieldName, start));
    return new MaskingEngine(List.of(binding));
  }

  private static MaskingPattern memoryPattern(String name, String prefix, long start) {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName(name);
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(MaskingToken.SEQUENCE);
    pattern.setStorage(MaskingStorage.MEMORY);
    pattern.setPrefix(prefix);
    pattern.setSequenceStart(Long.toString(start));
    return pattern;
  }

  private static String apply(MaskingEngine engine, RowMeta rowMeta, String value)
      throws Exception {
    Object[] row = new Object[] {value};
    engine.apply(rowMeta, row);
    return (String) row[0];
  }

  private static RowMeta stringRow(String name) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(name));
    return rowMeta;
  }

  private static String uniqueId() {
    return "runtime-" + UUID.randomUUID();
  }
}
