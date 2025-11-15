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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link org.apache.hop.core.row.RowMeta.RowMetaCache} */
class RowMetaCacheTest {
  private RowMeta.RowMetaCache cache;
  private List<IValueMeta> metas;

  @BeforeEach
  void setUp() {
    cache = new RowMeta.RowMetaCache();
    metas = new ArrayList<>();
  }

  @Test
  void testFindAndCompare() {
    cache.storeMapping("Price", 3);
    metas.addAll(List.of(meta("id"), meta("name"), meta("age"), meta("price")));

    Integer idx = cache.findAndCompare("PRICE", metas);
    assertEquals(3, idx);
    assertTrue(cache.mapping.containsKey("price"));
  }

  @Test
  void testInsertAtBeginning() {
    cache.storeMapping("id", 0);
    cache.storeMapping("name", 1);

    cache.insertAtMapping("sale", 0);
    assertEquals(3, cache.mapping.size());

    assertEquals(0, cache.mapping.get("sale"));
    assertEquals(1, cache.mapping.get("id"));
    assertEquals(2, cache.mapping.get("name"));
  }

  @Test
  void testInsertAtMiddle() {
    cache.storeMapping("id", 0);
    cache.storeMapping("name", 1);
    cache.storeMapping("age", 2);
    cache.storeMapping("city", 3);

    cache.insertAtMapping("sale", 2);
    assertEquals(5, cache.mapping.size());

    assertEquals(2, cache.mapping.get("sale"));
    assertEquals(3, cache.mapping.get("age"));
    assertEquals(4, cache.mapping.get("city"));
  }

  @Test
  void testInsertAtEnd() {
    cache.storeMapping("id", 0);
    cache.storeMapping("name", 1);

    cache.insertAtMapping("sale", 2);
    assertEquals(3, cache.mapping.size());

    assertEquals(0, cache.mapping.get("id"));
    assertEquals(1, cache.mapping.get("name"));
    assertEquals(2, cache.mapping.get("sale"));
  }

  private static IValueMeta meta(String name) {
    IValueMeta vm = mock(IValueMeta.class);
    when(vm.getName()).thenReturn(name);
    return vm;
  }
}
