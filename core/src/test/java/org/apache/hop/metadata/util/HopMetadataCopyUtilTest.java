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

package org.apache.hop.metadata.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.junit.jupiter.api.Test;

class HopMetadataCopyUtilTest {

  public static class Item {
    @HopMetadataProperty private String name;

    public Item() {}

    public Item(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class Nested {
    @HopMetadataProperty private String table;
    @HopMetadataProperty private List<Item> items = new ArrayList<>();

    public String getTable() {
      return table;
    }

    public void setTable(String table) {
      this.table = table;
    }

    public List<Item> getItems() {
      return items;
    }
  }

  public static class Holder {
    @HopMetadataProperty private String plain;
    @HopMetadataProperty private List<Item> items = new ArrayList<>();
    @HopMetadataProperty private Map<String, Item> byName = new LinkedHashMap<>();
    @HopMetadataProperty private Nested nested = new Nested();
    @HopMetadataProperty private Date date;
    @HopMetadataProperty private String[] tags;
    @HopMetadataProperty private Nested absent;

    // Not annotated: a live reference that must keep being shared.
    private Object liveReference;
    // Transient state is not persisted either, so it stays shared.
    @HopMetadataProperty private transient Object cache;
  }

  private static Holder populated() {
    Holder holder = new Holder();
    holder.plain = "original";
    holder.items.add(new Item("one"));
    holder.byName.put("a", new Item("a"));
    holder.nested.setTable("customers");
    holder.nested.getItems().add(new Item("nested-one"));
    holder.date = new Date(1_000_000L);
    holder.tags = new String[] {"x", "y"};
    holder.liveReference = new Object();
    holder.cache = new Object();
    return holder;
  }

  private static Holder copyOf(Holder source) {
    Holder target = new Holder();
    // Mimic Object.clone(): every field starts out shared with the source.
    target.plain = source.plain;
    target.items = source.items;
    target.byName = source.byName;
    target.nested = source.nested;
    target.date = source.date;
    target.tags = source.tags;
    target.absent = source.absent;
    target.liveReference = source.liveReference;
    target.cache = source.cache;

    HopMetadataCopyUtil.copyMetadataProperties(source, target);
    return target;
  }

  @Test
  void listsAreIndependent() {
    Holder source = populated();
    Holder copy = copyOf(source);

    assertNotSame(source.items, copy.items);
    assertNotSame(source.items.get(0), copy.items.get(0));

    source.items.clear();
    source.items.add(new Item("replaced"));

    assertEquals(1, copy.items.size());
    assertEquals("one", copy.items.get(0).getName());
  }

  @Test
  void nestedObjectsAreIndependent() {
    Holder source = populated();
    Holder copy = copyOf(source);

    assertNotSame(source.nested, copy.nested);
    source.nested.setTable("orders");
    source.nested.getItems().get(0).setName("changed");

    assertEquals("customers", copy.nested.getTable());
    assertEquals("nested-one", copy.nested.getItems().get(0).getName());
  }

  @Test
  void mapsArraysAndDatesAreIndependent() {
    Holder source = populated();
    Holder copy = copyOf(source);

    assertNotSame(source.byName, copy.byName);
    assertNotSame(source.byName.get("a"), copy.byName.get("a"));
    assertNotSame(source.tags, copy.tags);
    assertNotSame(source.date, copy.date);
    assertEquals(source.date, copy.date);

    source.byName.get("a").setName("changed");
    source.tags[0] = "changed";
    source.date.setTime(42L);

    assertEquals("a", copy.byName.get("a").getName());
    assertEquals("x", copy.tags[0]);
    assertEquals(1_000_000L, copy.date.getTime());
  }

  @Test
  void unannotatedAndTransientStateStaysShared() {
    Holder source = populated();
    Holder copy = copyOf(source);

    assertSame(source.liveReference, copy.liveReference);
    assertSame(source.cache, copy.cache);
  }

  @Test
  void nullPropertiesStayNull() {
    Holder source = populated();
    Holder copy = copyOf(source);

    assertNull(copy.absent);
  }
}
