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

import static org.apache.hop.core.row.RowMetaAssert.assertLookups;
import static org.apache.hop.core.row.RowMetaAssert.assertNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Long random sequences of every mutation of {@link RowMeta}, checked after each step against a
 * plain list of the names upstream gives the fields. Names come from a small pool with case
 * variants and suffixes, so duplicates, renames to {@code name_n} and in-place renames happen
 * often. Lookups are checked after some steps only: a lookup fills the name cache, and the
 * duplicate checks must also be right when nothing looked the names up.
 */
class RowMetaRandomOperationsTest {

  private static final int STEPS = 250;

  private static final String[] POOL = {
    "a", "A", "b", "B", "id", "ID", "Id", "name", "NAME", "a_1", "A_1", "a_2", "b_1", "x", "y", "z",
    "field", "FIELD_1"
  };

  static IntStream seeds() {
    return IntStream.range(0, 40);
  }

  /** The state of one run: the row under test and what it is expected to hold. */
  private static final class Run {
    final Random random;
    final List<String> names = new ArrayList<>();
    final List<IValueMeta> metas = new ArrayList<>();
    final Deque<String> log = new ArrayDeque<>();
    RowMeta rowMeta = new RowMeta();
    int fresh;

    Run(long seed) {
      random = new Random(seed);
    }

    void log(String operation) {
      log.addLast(operation);
      if (log.size() > 12) {
        log.removeFirst();
      }
    }

    String poolName() {
      return POOL[random.nextInt(POOL.length)];
    }

    IValueMeta newValueMeta(String name) {
      IValueMeta valueMeta =
          random.nextBoolean() ? new ValueMetaString(name) : new ValueMetaInteger(name);
      valueMeta.setOrigin("source");
      return valueMeta;
    }

    boolean taken(List<String> in, String name, int except) {
      for (int i = 0; i < in.size(); i++) {
        if (i != except && name.equalsIgnoreCase(in.get(i))) {
          return true;
        }
      }
      return false;
    }

    int indexOf(String name) {
      for (int i = 0; i < names.size(); i++) {
        if (name.equalsIgnoreCase(names.get(i))) {
          return i;
        }
      }
      return -1;
    }

    /** The name upstream gives a duplicate: the first free {@code name_n}, ignoring case. */
    String suffixed(String name) {
      int n = 1;
      while (taken(names, name + "_" + n, -1)) {
        n++;
      }
      return name + "_" + n;
    }

    /** Expected name of a value meta added at the given position, and the check after it. */
    void expectAdded(IValueMeta added, int position, String origin) {
      String name = added.getName();
      boolean duplicate = taken(names, name, -1);
      String expectedName = duplicate ? suffixed(name) : name;
      names.add(position, expectedName);
      IValueMeta actual = rowMeta.getValueMeta(position);
      assertEquals(expectedName, actual.getName(), "name of the added field");
      if (duplicate) {
        assertNotSame(added, actual, "a renamed field is a copy");
        assertEquals(name, added.getName(), "the value meta passed in keeps its name");
        assertEquals(origin == null ? added.getOrigin() : origin, actual.getOrigin());
      } else {
        assertSame(added, actual, "a field that is not renamed is added as is");
      }
      metas.add(position, actual);
    }

    List<IValueMeta> sourceMetas() {
      List<IValueMeta> source = new ArrayList<>();
      int count = 1 + random.nextInt(3);
      for (int i = 0; i < count; i++) {
        source.add(newValueMeta(poolName()));
      }
      return source;
    }

    void step(int step) throws Exception {
      int size = names.size();
      int operation = random.nextInt(100);
      if (operation < 20) {
        IValueMeta valueMeta = newValueMeta(poolName());
        log("addValueMeta(" + valueMeta.getName() + ")");
        rowMeta.addValueMeta(valueMeta);
        expectAdded(valueMeta, size, null);
      } else if (operation < 30) {
        int index = random.nextInt(size + 1);
        IValueMeta valueMeta = newValueMeta(poolName());
        log("addValueMeta(" + index + ", " + valueMeta.getName() + ")");
        rowMeta.addValueMeta(index, valueMeta);
        expectAdded(valueMeta, index, null);
      } else if (operation < 38) {
        if (size == 0) {
          return;
        }
        int index = random.nextInt(size);
        IValueMeta valueMeta = newValueMeta(poolName());
        log("setValueMeta(" + index + ", " + valueMeta.getName() + ")");
        rowMeta.setValueMeta(index, valueMeta);
        int existing = indexOf(valueMeta.getName());
        String expectedName =
            existing >= 0 && existing != index
                ? suffixed(valueMeta.getName())
                : valueMeta.getName();
        names.set(index, expectedName);
        IValueMeta actual = rowMeta.getValueMeta(index);
        assertEquals(expectedName, actual.getName(), "name of the replacing field");
        metas.set(index, actual);
      } else if (operation < 44) {
        if (size == 0) {
          return;
        }
        int index = random.nextInt(size);
        log("removeValueMeta(" + index + ")");
        rowMeta.removeValueMeta(index);
        names.remove(index);
        metas.remove(index);
      } else if (operation < 50) {
        if (size > 0 && random.nextInt(4) > 0) {
          int index = random.nextInt(size);
          String name = names.get(index);
          String variant =
              random.nextBoolean() ? name.toUpperCase(Locale.ROOT) : name.toLowerCase(Locale.ROOT);
          log("removeValueMeta(\"" + variant + "\")");
          rowMeta.removeValueMeta(variant);
          names.remove(index);
          metas.remove(index);
        } else {
          String missing = "missing_" + step;
          log("removeValueMeta(\"" + missing + "\")");
          assertThrows(HopValueException.class, () -> rowMeta.removeValueMeta(missing));
        }
      } else if (operation < 51) {
        log("clear()");
        rowMeta.clear();
        names.clear();
        metas.clear();
      } else if (operation < 56) {
        List<IValueMeta> source = sourceMetas();
        RowMeta other = new RowMeta();
        other.setValueMetaList(new ArrayList<>(source));
        log("addRowMeta(" + other + ")");
        rowMeta.addRowMeta(other);
        for (IValueMeta valueMeta : source) {
          expectAdded(valueMeta, names.size(), null);
        }
      } else if (operation < 61) {
        List<IValueMeta> source = sourceMetas();
        RowMeta other = new RowMeta();
        other.setValueMetaList(new ArrayList<>(source));
        String origin = random.nextBoolean() ? "merge" + step : null;
        log("mergeRowMeta(" + other + ", " + origin + ")");
        rowMeta.mergeRowMeta(other, origin);
        for (IValueMeta valueMeta : source) {
          expectAdded(valueMeta, names.size(), origin);
        }
      } else if (operation < 63) {
        List<IValueMeta> list = new ArrayList<>();
        List<String> listNames = new ArrayList<>();
        int count = random.nextInt(5);
        for (int i = 0; i < count; i++) {
          String name = poolName();
          if (!taken(listNames, name, -1)) {
            listNames.add(name);
            list.add(newValueMeta(name));
          }
        }
        log("setValueMetaList(" + listNames + ")");
        rowMeta.setValueMetaList(list);
        names.clear();
        names.addAll(listNames);
        metas.clear();
        metas.addAll(list);
      } else if (operation < 78) {
        if (size == 0) {
          return;
        }
        int index = random.nextInt(size);
        String old = names.get(index);
        String name;
        int kind = random.nextInt(10);
        if (kind < 6) {
          name = poolName();
          if (taken(names, name, index)) {
            name = "r" + (fresh++);
          }
        } else if (kind < 8) {
          String upper = old.toUpperCase(Locale.ROOT);
          name = upper.equals(old) ? old.toLowerCase(Locale.ROOT) : upper;
        } else {
          name = "r" + (fresh++);
        }
        log("getValueMeta(" + index + ").setName(" + old + " -> " + name + ")");
        rowMeta.getValueMeta(index).setName(name);
        names.set(index, name);
      } else if (operation < 81) {
        log("clone()");
        RowMeta original = rowMeta;
        RowMeta clone = original.clone();
        assertNames(clone, names.toArray(new String[0]));
        for (int i = 0; i < size; i++) {
          assertNotSame(original.getValueMeta(i), clone.getValueMeta(i));
          metas.set(i, clone.getValueMeta(i));
        }
        if (size > 0) {
          int index = random.nextInt(size);
          String name = "c" + (fresh++);
          log("clone.getValueMeta(" + index + ").setName(" + name + ")");
          clone.getValueMeta(index).setName(name);
          assertEquals(names.get(index), original.getValueMeta(index).getName());
          names.set(index, name);
        }
        rowMeta = clone;
      } else {
        for (int i = 0; i < 3; i++) {
          String name = poolName();
          log("indexOfValue(" + name + ")");
          assertEquals(indexOf(name), rowMeta.indexOfValue(name), "indexOfValue(" + name + ")");
        }
      }
    }

    void check(boolean withLookups) {
      assertNames(rowMeta, names.toArray(new String[0]));
      for (int i = 0; i < metas.size(); i++) {
        assertSame(metas.get(i), rowMeta.getValueMeta(i), "value meta at " + i);
      }
      if (withLookups) {
        assertLookups(rowMeta);
        for (String name : POOL) {
          if (indexOf(name) < 0) {
            assertEquals(-1, rowMeta.indexOfValue(name), "indexOfValue(" + name + ")");
            assertNull(rowMeta.searchValueMeta(name), "searchValueMeta(" + name + ")");
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "seed {0}")
  @MethodSource("seeds")
  void randomSequencesOfMutationsAndRenames(int seed) throws Exception {
    Run run = new Run(seed);
    for (int step = 0; step < STEPS; step++) {
      try {
        run.step(step);
        run.check(run.random.nextInt(10) < 3);
      } catch (AssertionError e) {
        throw new AssertionError(
            "seed "
                + seed
                + ", step "
                + step
                + ", last operations: "
                + String.join(" ; ", run.log)
                + System.lineSeparator()
                + "expected names "
                + run.names
                + System.lineSeparator()
                + "actual names   "
                + List.of(run.rowMeta.getFieldNames()),
            e);
      }
    }
    run.check(true);
  }
}
