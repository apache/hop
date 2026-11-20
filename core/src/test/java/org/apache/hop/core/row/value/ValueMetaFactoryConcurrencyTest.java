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

package org.apache.hop.core.row.value;

import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.BUILT_IN_NAMES_IN_ORDER;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_A;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.unknownTypeMessage;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.valueMetaPlugin;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaTestA;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaTestB;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Many threads ask {@link ValueMetaFactory} for value types while another thread registers,
 * replaces and removes a test type. The built-in types must always come back right, and the test
 * type is either there with one of its classes or absent with the usual exception.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaFactoryConcurrencyTest {

  private static final String NAME_A = "Hop9606 Concurrent";
  private static final int READERS = 8;
  private static final int WRITER_ROUNDS = 400;

  private final IPlugin pluginA =
      valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A));
  private final IPlugin pluginB =
      valueMetaPlugin(NAME_A, ValueMetaTestB.class, String.valueOf(TYPE_A));

  /** What the factory answers for a built-in type, taken before the threads start. */
  private record Expected(int type, Class<?> valueMetaClass, String name, int precision) {}

  @AfterEach
  void removeTestPlugin() {
    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, pluginA);
    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, pluginB);
  }

  @Test
  void readersAlwaysGetCorrectAnswersWhileAWriterChangesTheRegistry() throws Exception {
    List<Expected> expected = new ArrayList<>();
    for (String name : BUILT_IN_NAMES_IN_ORDER) {
      int type = ValueMetaFactory.getIdForValueMeta(name);
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta("f", type, 7, 3);
      expected.add(new Expected(type, valueMeta.getClass(), name, valueMeta.getPrecision()));
    }
    String[] selectable = ValueMetaFactory.getValueMetaNames();

    Queue<String> failures = new ConcurrentLinkedQueue<>();
    AtomicBoolean writerDone = new AtomicBoolean();
    AtomicInteger testTypeSeen = new AtomicInteger();
    AtomicInteger testTypeMissing = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(READERS + 1);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int r = 0; r < READERS; r++) {
        futures.add(
            executor.submit(
                () -> {
                  await(start);
                  int rounds = 0;
                  while (!writerDone.get() || rounds < 50) {
                    rounds++;
                    readOnce(expected, selectable, failures, testTypeSeen, testTypeMissing);
                    if (failures.size() > 20) {
                      return;
                    }
                  }
                }));
      }
      futures.add(
          executor.submit(
              () -> {
                await(start);
                try {
                  for (int i = 0; i < WRITER_ROUNDS; i++) {
                    PluginRegistry registry = PluginRegistry.getInstance();
                    registry.registerPlugin(ValueMetaPluginType.class, pluginA);
                    checkWriterView(ValueMetaTestA.class, failures);
                    // Same name and ids: replaces A
                    registry.registerPlugin(ValueMetaPluginType.class, pluginB);
                    checkWriterView(ValueMetaTestB.class, failures);
                    registry.removePlugin(ValueMetaPluginType.class, pluginB);
                    checkWriterView(null, failures);
                  }
                } catch (Throwable t) {
                  failures.add("writer: " + t);
                } finally {
                  writerDone.set(true);
                }
              }));
      start.countDown();
      for (Future<?> future : futures) {
        future.get(5, TimeUnit.MINUTES);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    }

    assertEquals(List.of(), List.copyOf(failures));
    assertTrue(testTypeSeen.get() + testTypeMissing.get() > 0);

    // Once the writer is done the type is gone, for every method
    HopPluginException e =
        assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));
    assertEquals(unknownTypeMessage(TYPE_A), e.getMessage());
    assertEquals("-", ValueMetaFactory.getValueMetaName(TYPE_A));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(NAME_A));
    assertEquals(Arrays.asList(selectable), Arrays.asList(ValueMetaFactory.getValueMetaNames()));

    // ... and registering it once more makes it usable right away
    PluginRegistry.getInstance().registerPlugin(ValueMetaPluginType.class, pluginB);
    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta(NAME_A));
  }

  /** Right after its own change, the writer must see exactly that change. */
  private static void checkWriterView(Class<?> expectedClass, Queue<String> failures) {
    try {
      if (expectedClass == null) {
        try {
          IValueMeta valueMeta = ValueMetaFactory.createValueMeta(TYPE_A);
          failures.add("writer: removed type still created " + valueMeta.getClass());
        } catch (HopPluginException e) {
          if (!unknownTypeMessage(TYPE_A).equals(e.getMessage())) {
            failures.add("writer: unexpected message " + e.getMessage());
          }
        }
        if (ValueMetaFactory.getIdForValueMeta(NAME_A) != IValueMeta.TYPE_NONE) {
          failures.add("writer: removed type still found by name");
        }
      } else {
        Class<?> actual = ValueMetaFactory.createValueMeta(TYPE_A).getClass();
        if (actual != expectedClass) {
          failures.add("writer: expected " + expectedClass + " but got " + actual);
        }
        if (ValueMetaFactory.getIdForValueMeta(NAME_A) != TYPE_A) {
          failures.add("writer: registered type not found by name");
        }
        if (!NAME_A.equals(ValueMetaFactory.getValueMetaName(TYPE_A))) {
          failures.add("writer: registered type has no name");
        }
      }
    } catch (Throwable t) {
      failures.add("writer: " + t);
    }
  }

  private static void readOnce(
      List<Expected> expected,
      String[] selectable,
      Queue<String> failures,
      AtomicInteger testTypeSeen,
      AtomicInteger testTypeMissing) {
    try {
      for (Expected e : expected) {
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta("f", e.type(), 7, 3);
        if (valueMeta.getClass() != e.valueMetaClass()
            || !"f".equals(valueMeta.getName())
            || valueMeta.getLength() != 7
            || valueMeta.getPrecision() != e.precision()
            || valueMeta.getType() != e.type()) {
          failures.add(
              "type "
                  + e.type()
                  + ": "
                  + valueMeta.getClass()
                  + " "
                  + valueMeta.getName()
                  + " "
                  + valueMeta.getLength()
                  + " "
                  + valueMeta.getPrecision());
        }
        int id = ValueMetaFactory.getIdForValueMeta(e.name());
        if (id != e.type()) {
          failures.add("id of " + e.name() + ": " + id);
        }
        String upperId = e.name().toUpperCase(Locale.ROOT);
        if (ValueMetaFactory.getIdForValueMeta(upperId) != e.type()) {
          failures.add("id of " + upperId);
        }
        String name = ValueMetaFactory.getValueMetaName(e.type());
        if (!e.name().equals(name)) {
          failures.add("name of " + e.type() + ": " + name);
        }
      }

      List<String> names = new ArrayList<>(Arrays.asList(ValueMetaFactory.getValueMetaNames()));
      names.remove(NAME_A);
      if (!names.equals(Arrays.asList(selectable))) {
        failures.add("names: " + names);
      }

      try {
        IValueMeta testType = ValueMetaFactory.createValueMeta("t", TYPE_A);
        if (testType.getClass() != ValueMetaTestA.class
            && testType.getClass() != ValueMetaTestB.class) {
          failures.add("test type: " + testType.getClass());
        } else if (!"t".equals(testType.getName())) {
          failures.add("test type name: " + testType.getName());
        }
        testTypeSeen.incrementAndGet();
      } catch (HopPluginException ex) {
        if (!unknownTypeMessage(TYPE_A).equals(ex.getMessage())) {
          failures.add("test type message: " + ex.getMessage());
        }
        testTypeMissing.incrementAndGet();
      }
      int testId = ValueMetaFactory.getIdForValueMeta(NAME_A);
      if (testId != TYPE_A && testId != IValueMeta.TYPE_NONE) {
        failures.add("test type id: " + testId);
      }
      String testName = ValueMetaFactory.getValueMetaName(TYPE_A);
      if (!NAME_A.equals(testName) && !"-".equals(testName)) {
        failures.add("test type name lookup: " + testName);
      }
    } catch (Throwable t) {
      failures.add(Thread.currentThread().getName() + ": " + t);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void concurrentFirstUseOfManyTypesGivesTheRightClasses() throws Exception {
    Map<Integer, Class<?>> expected = new HashMap<>();
    for (String name : BUILT_IN_NAMES_IN_ORDER) {
      int type = ValueMetaFactory.getIdForValueMeta(name);
      expected.put(type, ValueMetaFactory.createValueMeta(type).getClass());
    }
    Queue<String> failures = new ConcurrentLinkedQueue<>();
    ExecutorService executor = Executors.newFixedThreadPool(READERS);
    try {
      for (int round = 0; round < 50; round++) {
        // Every round changes the registry, so whatever the factory remembers starts over
        PluginRegistry.getInstance().registerPlugin(ValueMetaPluginType.class, pluginA);
        PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, pluginA);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int r = 0; r < READERS; r++) {
          final int offset = r;
          futures.add(
              executor.submit(
                  () -> {
                    await(start);
                    List<Integer> types = new ArrayList<>(expected.keySet());
                    for (int i = 0; i < types.size(); i++) {
                      int type = types.get((i + offset) % types.size());
                      try {
                        Class<?> actual = ValueMetaFactory.createValueMeta(type).getClass();
                        if (actual != expected.get(type)) {
                          failures.add(type + ": " + actual);
                        }
                      } catch (Throwable t) {
                        failures.add(type + ": " + t);
                      }
                    }
                  }));
        }
        start.countDown();
        for (Future<?> future : futures) {
          future.get(1, TimeUnit.MINUTES);
        }
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    }
    assertEquals(List.of(), List.copyOf(failures));
  }
}
