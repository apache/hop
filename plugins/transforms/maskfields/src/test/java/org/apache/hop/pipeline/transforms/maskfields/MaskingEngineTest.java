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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.maskfields.MaskingEngine.Binding;
import org.apache.hop.pipeline.transforms.maskfields.store.MemoryMaskingStore;
import org.junit.jupiter.api.Test;

class MaskingEngineTest {

  @Test
  void syntheticWithoutMemoryAssignsANewTokenEveryRow() throws Exception {
    MaskingPattern pattern = synthetic("First name", MaskingStorage.NONE, "first-name-", "", "5");
    MaskingEngine engine = engine(binding("name", pattern, null));
    RowMeta rowMeta = stringRow("name");

    assertEquals("first-name-5", apply(engine, rowMeta, "Matt")[0]);
    assertEquals("first-name-6", apply(engine, rowMeta, "Matt")[0]);
  }

  @Test
  void uuidTokenHasTheStandardShape() throws Exception {
    MaskingPattern pattern = synthetic("Id", MaskingStorage.NONE, "id-", "-x", "1");
    pattern.setToken(MaskingToken.UUID);
    MaskingEngine engine = engine(binding("name", pattern, null));
    String masked = (String) apply(engine, stringRow("name"), "Matt")[0];
    assertTrue(masked.startsWith("id-"));
    assertTrue(masked.endsWith("-x"));
    UUID.fromString(masked.substring("id-".length(), masked.length() - 2));
  }

  @Test
  void memoryKeepsTheSameTokenAndSharesItAcrossFields() throws Exception {
    MaskingPattern pattern = synthetic("First name", MaskingStorage.MEMORY, "first-name-", "", "1");
    MemoryMaskingStore store = new MemoryMaskingStore();
    MaskingEngine engine =
        engine(binding("first", pattern, store), binding("nickname", pattern, store));
    RowMeta rowMeta = stringRow("first", "nickname");

    Object[] first = apply(engine, rowMeta, "Matt", "Matt");
    assertEquals("first-name-1", first[0]);
    assertEquals("first-name-1", first[1]);

    Object[] second = apply(engine, rowMeta, "Ann", "Matt");
    assertEquals("first-name-2", second[0]);
    assertEquals("first-name-1", second[1]);
  }

  @Test
  void nullAndEmptyPassThrough() throws Exception {
    MaskingPattern pattern = synthetic("First name", MaskingStorage.MEMORY, "first-name-", "", "1");
    MaskingEngine engine = engine(binding("name", pattern, new MemoryMaskingStore()));
    RowMeta rowMeta = stringRow("name");
    assertNull(apply(engine, rowMeta, new Object[] {null})[0]);
    assertEquals("", apply(engine, rowMeta, "")[0]);
    assertEquals("first-name-1", apply(engine, rowMeta, "Matt")[0]);
  }

  @Test
  void setNullAndSetEmpty() throws Exception {
    MaskingPattern clear = new MaskingPattern();
    clear.setName("Clear");
    clear.setValueSource(MaskingValueSource.SET_NULL);
    MaskingPattern blank = new MaskingPattern();
    blank.setName("Blank");
    blank.setValueSource(MaskingValueSource.SET_EMPTY);
    MaskingEngine engine = engine(binding("a", clear, null), binding("b", blank, null));
    RowMeta rowMeta = stringRow("a", "b");
    Object[] masked = apply(engine, rowMeta, "secret", "secret");
    assertNull(masked[0]);
    assertEquals("", masked[1]);
  }

  private static MaskingPattern synthetic(
      String name, MaskingStorage storage, String prefix, String suffix, String start) {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName(name);
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(MaskingToken.SEQUENCE);
    pattern.setStorage(storage);
    pattern.setPrefix(prefix);
    pattern.setSuffix(suffix);
    pattern.setSequenceStart(start);
    return pattern;
  }

  private static Binding binding(String field, MaskingPattern pattern, MemoryMaskingStore store) {
    long start = 1L;
    if (pattern.getSequenceStart() != null && !pattern.getSequenceStart().isBlank()) {
      start = Long.parseLong(pattern.getSequenceStart().trim());
    }
    return new Binding(field, pattern, pattern.getPrefix(), pattern.getSuffix(), start, store);
  }

  private static MaskingEngine engine(Binding... bindings) {
    return new MaskingEngine(List.of(bindings));
  }

  private static RowMeta stringRow(String... names) {
    RowMeta rowMeta = new RowMeta();
    for (String name : names) {
      rowMeta.addValueMeta(new ValueMetaString(name));
    }
    return rowMeta;
  }

  private static Object[] apply(MaskingEngine engine, RowMeta rowMeta, Object... values)
      throws Exception {
    Object[] row = values.clone();
    engine.apply(rowMeta, row);
    return row;
  }
}
