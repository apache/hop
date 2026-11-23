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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IClassLoadingPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;

/**
 * Value meta types and plugins that the {@link ValueMetaFactory} tests register at runtime. The
 * type ids are far away from the built-in ones (0-11, 20) so they never collide with them.
 */
final class ValueMetaFactoryTestPlugins {

  static final int TYPE_A = 9606001;
  static final int TYPE_MULTI_MAIN = 9606003;
  static final int TYPE_MULTI_SECOND = 9606004;
  static final int TYPE_COUNTING = 9606005;
  static final int TYPE_CLASS_LOADING = 9606006;
  static final int TYPE_FACTORY = 9606007;
  static final int TYPE_ANNOTATED = 9606050;

  /** The built-in value meta plugins, in the order the registry lists them (by name). */
  static final List<String> BUILT_IN_NAMES_IN_ORDER =
      List.of(
          "Avro Record",
          "BigNumber",
          "Binary",
          "Boolean",
          "Date",
          "Integer",
          "Internet Address",
          "JSON",
          "None",
          "Number",
          "Serializable",
          "String",
          "Timestamp");

  private ValueMetaFactoryTestPlugins() {
    // Constants and helpers only
  }

  /** The message of the exception {@link ValueMetaFactory#createValueMeta} throws for an id. */
  static String unknownTypeMessage(int type) {
    return Const.CR + "Unable to locate value meta plugin of type (id) " + type + Const.CR;
  }

  /** A plain plugin, as the registry builds one from a {@code @ValueMetaPlugin} annotation. */
  static Plugin valueMetaPlugin(String name, Class<?> valueMetaClass, String... ids) {
    Map<Class<?>, String> classMap = new HashMap<>();
    if (valueMetaClass != null) {
      classMap.put(IValueMeta.class, valueMetaClass.getName());
    }
    return valueMetaPlugin(name, classMap, ids);
  }

  static Plugin valueMetaPlugin(String name, Map<Class<?>, String> classMap, String... ids) {
    return new Plugin(
        ids,
        ValueMetaPluginType.class,
        IValueMeta.class,
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

  static void register(IPlugin plugin) throws HopPluginException {
    PluginRegistry.getInstance().registerPlugin(ValueMetaPluginType.class, plugin);
  }

  static void remove(IPlugin plugin) {
    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, plugin);
  }

  /** A new value type: type id {@link #TYPE_A}. */
  public static class ValueMetaTestA extends ValueMetaBase {
    public ValueMetaTestA() {
      super(null, TYPE_A);
    }
  }

  /** Another class for the same type id as {@link ValueMetaTestA}. */
  public static class ValueMetaTestB extends ValueMetaBase {
    public ValueMetaTestB() {
      super(null, TYPE_A);
    }
  }

  /** A third class for the same type id as {@link ValueMetaTestA}. */
  public static class ValueMetaTestC extends ValueMetaBase {
    public ValueMetaTestC() {
      super(null, TYPE_A);
    }
  }

  /** The class of a plugin with several ids; it always reports the first one as its type. */
  public static class ValueMetaTestMulti extends ValueMetaBase {
    public ValueMetaTestMulti() {
      super(null, TYPE_MULTI_MAIN);
    }
  }

  /** Registered through its annotation, as the registry does for the built-in types. */
  @ValueMetaPlugin(id = "9606050", name = "Hop9606 Annotated", description = "Annotated test type")
  public static class ValueMetaAnnotated extends ValueMetaBase {
    public ValueMetaAnnotated() {
      super(null, TYPE_ANNOTATED);
    }
  }

  /** Counts how often its constructor runs and fails on demand. */
  public static class ValueMetaCounting extends ValueMetaBase {
    static final AtomicInteger CONSTRUCTED = new AtomicInteger();
    static volatile boolean failing;

    public ValueMetaCounting() {
      super(null, TYPE_COUNTING);
      CONSTRUCTED.incrementAndGet();
      if (failing) {
        throw new IllegalStateException("ValueMetaCounting was told to fail");
      }
    }
  }

  /** Has no constructor the plugin registry may call. */
  public static class ValueMetaPrivateConstructor extends ValueMetaBase {
    private ValueMetaPrivateConstructor() {
      super(null, TYPE_A);
    }
  }

  /** Can't be instantiated at all. */
  public abstract static class ValueMetaAbstract extends ValueMetaBase {
    public ValueMetaAbstract() {
      super(null, TYPE_A);
    }
  }

  /**
   * A plugin that loads its own classes: the registry asks it for every class instead of using a
   * class map. It counts how often it is asked and hands out whatever the supplier returns.
   */
  static final class CountingClassLoadingPlugin extends Plugin implements IClassLoadingPlugin {
    final AtomicInteger loadClassCalls = new AtomicInteger();
    private volatile Supplier<?> supplier;

    CountingClassLoadingPlugin(String name, Supplier<?> supplier, String... ids) {
      super(
          ids,
          ValueMetaPluginType.class,
          IValueMeta.class,
          null,
          name,
          name,
          null,
          false,
          true,
          new HashMap<>(),
          new ArrayList<>(),
          null,
          null,
          null,
          false);
      this.supplier = supplier;
    }

    void setSupplier(Supplier<?> supplier) {
      this.supplier = supplier;
    }

    @Override
    public <T> T loadClass(Class<T> pluginClass) {
      loadClassCalls.incrementAndGet();
      if (pluginClass != IValueMeta.class) {
        return null;
      }
      return pluginClass.cast(supplier.get());
    }

    @Override
    public ClassLoader getClassLoader() {
      return getClass().getClassLoader();
    }
  }
}
