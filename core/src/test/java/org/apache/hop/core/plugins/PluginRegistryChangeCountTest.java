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

package org.apache.hop.core.plugins;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** {@link PluginRegistry#getChangeCount()} moves on every change and only then. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class PluginRegistryChangeCountTest {

  /** A plugin type of its own, so the tests don't disturb the plugins of a real type. */
  private abstract static class ChangeCountTestPluginType implements IPluginType {}

  private final PluginRegistry registry = PluginRegistry.getInstance();
  private final List<IPlugin> registered = new ArrayList<>();

  @AfterEach
  void removeTestPlugins() {
    for (IPlugin plugin : registered) {
      registry.removePlugin(plugin.getPluginType(), plugin);
    }
    for (IPlugin plugin : registry.getPlugins(ValueMetaPluginType.class)) {
      if (plugin instanceof SupplementalPlugin) {
        registry.removePlugin(ValueMetaPluginType.class, plugin);
      }
    }
  }

  private static Plugin plugin(String id, String name, Class<?> mainClass) {
    Map<Class<?>, String> classMap = new HashMap<>();
    classMap.put(Object.class, mainClass.getName());
    return new Plugin(
        new String[] {id},
        ChangeCountTestPluginType.class,
        Object.class,
        null,
        name,
        name,
        null,
        false,
        true,
        classMap,
        new ArrayList<>(),
        null,
        null,
        null,
        false);
  }

  private Plugin registerTestPlugin(String id, String name, Class<?> mainClass)
      throws HopPluginException {
    Plugin plugin = plugin(id, name, mainClass);
    registered.add(plugin);
    registry.registerPlugin(ChangeCountTestPluginType.class, plugin);
    return plugin;
  }

  private long changesDuring(ThrowingRunnable action) throws Exception {
    long before = registry.getChangeCount();
    action.run();
    long after = registry.getChangeCount();
    assertTrue(after >= before, "the change count went back from " + before + " to " + after);
    return after - before;
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @Test
  void registeringANewPluginChangesTheCount() throws Exception {
    assertTrue(changesDuring(() -> registerTestPlugin("cc-1", "Change count 1", String.class)) > 0);
    assertTrue(changesDuring(() -> registerTestPlugin("cc-2", "Change count 2", String.class)) > 0);
  }

  @Test
  void registeringTheSamePluginAgainChangesTheCount() throws Exception {
    Plugin plugin = registerTestPlugin("cc-1", "Change count 1", String.class);
    assertTrue(
        changesDuring(() -> registry.registerPlugin(ChangeCountTestPluginType.class, plugin)) > 0);
  }

  @Test
  void replacingAPluginWithAnotherClassChangesTheCount() throws Exception {
    registerTestPlugin("cc-1", "Change count 1", String.class);
    // Same name and ids: the registry replaces the plugin
    assertTrue(
        changesDuring(() -> registerTestPlugin("cc-1", "Change count 1", Integer.class)) > 0);
    assertEquals(
        Integer.class.getName(),
        registry
            .getPlugin(ChangeCountTestPluginType.class, "cc-1")
            .getClassMap()
            .get(Object.class));
  }

  @Test
  void removingAPluginChangesTheCount() throws Exception {
    Plugin plugin = registerTestPlugin("cc-1", "Change count 1", String.class);
    assertTrue(
        changesDuring(() -> registry.removePlugin(ChangeCountTestPluginType.class, plugin)) > 0);
  }

  @Test
  void registeringAPluginTypeChangesTheCount() throws Exception {
    abstract class AnotherTestPluginType implements IPluginType {}
    assertTrue(changesDuring(() -> registry.registerPluginType(AnotherTestPluginType.class)) > 0);
  }

  @Test
  void addingParentClassLoaderPatternsChangesTheCount() throws Exception {
    Plugin plugin = registerTestPlugin("cc-1", "Change count 1", String.class);
    assertTrue(
        changesDuring(
                () -> registry.addParentClassLoaderPatterns(plugin, new String[] {"org.example"}))
            > 0);
  }

  @Test
  void addingTheFirstClassFactoryOfAPluginChangesTheCount() throws Exception {
    registerTestPlugin("cc-1", "Change count 1", String.class);
    assertTrue(
        changesDuring(
                () ->
                    registry.addClassFactory(
                        ChangeCountTestPluginType.class, Integer.class, "cc-1", () -> 1))
            > 0);
    IPlugin supplemental =
        registry.getPlugin(
            ChangeCountTestPluginType.class,
            ChangeCountTestPluginType.class.getName() + "-cc-1-supplementals");
    assertNotNull(supplemental);
    registered.add(supplemental);
  }

  @Test
  void resettingTheRegistryChangesTheCount() throws Exception {
    try {
      assertTrue(changesDuring(registry::reset) > 0);
    } finally {
      HopClientEnvironment.reset();
      HopClientEnvironment.init();
    }
  }

  @Test
  void initializingTheEnvironmentAgainChangesTheCount() throws Exception {
    HopClientEnvironment.reset();
    assertTrue(changesDuring(HopClientEnvironment::init) > 0);
  }

  @Test
  void aListenerAlreadySeesTheNewCount() throws Exception {
    List<Long> seen = new ArrayList<>();
    registry.addPluginListener(
        ChangeCountTestPluginType.class,
        new IPluginTypeListener() {
          @Override
          public void pluginAdded(Object serviceObject) {
            seen.add(registry.getChangeCount());
          }

          @Override
          public void pluginRemoved(Object serviceObject) {
            seen.add(registry.getChangeCount());
          }

          @Override
          public void pluginChanged(Object serviceObject) {
            seen.add(registry.getChangeCount());
          }
        });

    long beforeAdd = registry.getChangeCount();
    Plugin plugin = registerTestPlugin("cc-1", "Change count 1", String.class);
    long beforeChange = registry.getChangeCount();
    registry.registerPlugin(ChangeCountTestPluginType.class, plugin);
    long beforeRemove = registry.getChangeCount();
    registry.removePlugin(ChangeCountTestPluginType.class, plugin);

    assertEquals(3, seen.size());
    assertTrue(seen.get(0) > beforeAdd);
    assertTrue(seen.get(1) > beforeChange);
    assertTrue(seen.get(2) > beforeRemove);
  }

  @Test
  void readOnlyCallsDoNotChangeTheCount() throws Exception {
    Plugin plugin = registerTestPlugin("cc-1", "Change count 1", String.class);
    IPlugin stringPlugin = registry.getPlugin(ValueMetaPluginType.class, "2");
    assertNotNull(stringPlugin);
    // Anything created lazily on first use is created now
    registry.loadClass(stringPlugin, IValueMeta.class);
    registry.getClassLoader(stringPlugin);
    ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING);

    assertEquals(
        0,
        changesDuring(
            () -> {
              registry.getChangeCount();
              registry.getPluginTypes();
              registry.getPlugins(ChangeCountTestPluginType.class);
              registry.getPlugins(ValueMetaPluginType.class);
              registry.getPlugin(ChangeCountTestPluginType.class, "cc-1");
              registry.getPlugin(ChangeCountTestPluginType.class, "unknown");
              registry.findPluginWithId(ChangeCountTestPluginType.class, "cc-1");
              registry.findPluginWithName(ChangeCountTestPluginType.class, "Change count 1");
              registry.findPluginWithDescription(ChangeCountTestPluginType.class, "Change count 1");
              registry.getPluginsByCategory(ChangeCountTestPluginType.class, null);
              registry.findPluginIdWithMainClassName(
                  ValueMetaPluginType.class, ValueMetaString.class.getName());
              registry.getPluginId(ValueMetaPluginType.class, new ValueMetaString());
              registry.getPlugin(ValueMetaPluginType.class, new ValueMetaString());
              registry.loadClass(stringPlugin, IValueMeta.class);
              registry.loadClass(stringPlugin);
              registry.loadClass(ValueMetaPluginType.class, "2", IValueMeta.class);
              registry.loadClass(plugin, Object.class);
              registry.getClassLoader(stringPlugin);
              registry.getPluginInformation(ValueMetaPluginType.class);
              ValueMetaFactory.createValueMeta("s", IValueMeta.TYPE_STRING, 1, 0);
              ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING);
              ValueMetaFactory.getIdForValueMeta("String");
              ValueMetaFactory.getValueMetaNames();
              ValueMetaFactory.getAllValueMetaNames();
              ValueMetaFactory.getValueMetaPluginClasses();
            }));
  }

  @Test
  void concurrentChangesAreAllCounted() throws Exception {
    int threads = 8;
    int perThread = 50;
    List<Plugin> plugins = new ArrayList<>();
    for (int i = 0; i < threads * perThread; i++) {
      Plugin plugin = plugin("cc-c-" + i, "Change count concurrent " + i, String.class);
      plugins.add(plugin);
      registered.add(plugin);
    }
    long before = registry.getChangeCount();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        List<Plugin> mine = plugins.subList(t * perThread, (t + 1) * perThread);
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (Plugin plugin : mine) {
                    registry.registerPlugin(ChangeCountTestPluginType.class, plugin);
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(1, TimeUnit.MINUTES);
      }
    } finally {
      executor.shutdownNow();
    }

    assertTrue(
        registry.getChangeCount() - before >= (long) threads * perThread,
        "every registration moves the count");
    assertEquals(threads * perThread, registry.getPlugins(ChangeCountTestPluginType.class).size());
  }
}
