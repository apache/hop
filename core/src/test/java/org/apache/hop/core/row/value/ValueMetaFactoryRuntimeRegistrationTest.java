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
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_ANNOTATED;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_CLASS_LOADING;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_COUNTING;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_FACTORY;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_MULTI_MAIN;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_MULTI_SECOND;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.unknownTypeMessage;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.valueMetaPlugin;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopPluginClassMapException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.SupplementalPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.CountingClassLoadingPlugin;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaAbstract;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaAnnotated;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaCounting;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaPrivateConstructor;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaTestA;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaTestB;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaTestC;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaTestMulti;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Pins how {@link ValueMetaFactory} follows the plugin registry when value meta plugins are
 * registered, replaced and removed at runtime. Every test first uses the factory, so whatever it
 * may remember about the registry has been filled in before the registry changes.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaFactoryRuntimeRegistrationTest {

  private static final String NAME_A = "Hop9606 Type A";

  private static final List<String> BUILT_IN_SELECTABLE =
      BUILT_IN_NAMES_IN_ORDER.stream()
          .filter(n -> !n.equals("None") && !n.equals("Serializable"))
          .toList();

  private final List<IPlugin> registered = new ArrayList<>();

  @BeforeEach
  void useTheFactoryFirst() throws Exception {
    assertBuiltInsOnly();
    useEveryMethod();
  }

  @AfterEach
  void removeTestPlugins() {
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin plugin : registered) {
      registry.removePlugin(ValueMetaPluginType.class, plugin);
    }
    registered.clear();
    for (IPlugin plugin : registry.getPlugins(ValueMetaPluginType.class)) {
      if (plugin instanceof SupplementalPlugin || plugin.getIds()[0].contains("9606")) {
        registry.removePlugin(ValueMetaPluginType.class, plugin);
      }
    }
    ValueMetaCounting.failing = false;
    ValueMetaCounting.CONSTRUCTED.set(0);
    assertBuiltInsOnly();
  }

  private void register(IPlugin plugin) throws HopPluginException {
    registered.add(plugin);
    ValueMetaFactoryTestPlugins.register(plugin);
  }

  private static void assertBuiltInsOnly() {
    assertEquals(
        BUILT_IN_NAMES_IN_ORDER,
        PluginRegistry.getInstance().getPlugins(ValueMetaPluginType.class).stream()
            .map(IPlugin::getName)
            .toList());
  }

  /** Calls every factory method, for the built-in types and for the test types. */
  private static void useEveryMethod() throws Exception {
    for (int type : new int[] {IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER}) {
      ValueMetaFactory.createValueMeta(type);
      ValueMetaFactory.createValueMeta("x", type);
      ValueMetaFactory.createValueMeta("x", type, 1, 1);
    }
    for (int type :
        new int[] {
          TYPE_A,
          TYPE_MULTI_MAIN,
          TYPE_MULTI_SECOND,
          TYPE_COUNTING,
          TYPE_CLASS_LOADING,
          TYPE_FACTORY,
          TYPE_ANNOTATED
        }) {
      try {
        ValueMetaFactory.createValueMeta(type);
      } catch (HopPluginException e) {
        // Expected while the type isn't registered
      }
      ValueMetaFactory.getValueMetaName(type);
    }
    ValueMetaFactory.getIdForValueMeta("String");
    ValueMetaFactory.getIdForValueMeta(NAME_A);
    ValueMetaFactory.getValueMetaNames();
    ValueMetaFactory.getAllValueMetaNames();
    ValueMetaFactory.getValueMetaPluginClasses();
    ValueMetaFactory.cloneValueMeta(new ValueMetaString("s"), IValueMeta.TYPE_INTEGER);
  }

  private static List<String> withName(List<String> names, String name) {
    List<String> result = new ArrayList<>(names);
    result.add(name);
    result.sort(String::compareToIgnoreCase);
    return result;
  }

  private static void assertUnknown(int type) {
    for (HopPluginException e :
        List.of(
            assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(type)),
            assertThrows(
                HopPluginException.class, () -> ValueMetaFactory.createValueMeta("f", type)),
            assertThrows(
                HopPluginException.class,
                () -> ValueMetaFactory.createValueMeta("f", type, 1, 2)))) {
      assertSame(HopPluginException.class, e.getClass());
      assertEquals(unknownTypeMessage(type), e.getMessage());
    }
    assertEquals("-", ValueMetaFactory.getValueMetaName(type));
  }

  // A new type
  //

  @Test
  void aNewTypeIsUsableImmediatelyThroughEveryMethod() throws Exception {
    assertUnknown(TYPE_A);
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(NAME_A));

    register(valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A)));

    IValueMeta byType = ValueMetaFactory.createValueMeta(TYPE_A);
    assertSame(ValueMetaTestA.class, byType.getClass());
    assertEquals(TYPE_A, byType.getType());
    assertNull(byType.getName());
    assertEquals(-1, byType.getLength());
    assertEquals(-1, byType.getPrecision());

    IValueMeta byName = ValueMetaFactory.createValueMeta("named", TYPE_A);
    assertSame(ValueMetaTestA.class, byName.getClass());
    assertEquals("named", byName.getName());
    assertEquals(-1, byName.getLength());

    IValueMeta withLength = ValueMetaFactory.createValueMeta("sized", TYPE_A, 5, 2);
    assertSame(ValueMetaTestA.class, withLength.getClass());
    assertEquals("sized", withLength.getName());
    assertEquals(5, withLength.getLength());
    assertEquals(2, withLength.getPrecision());

    assertNotSame(byType, ValueMetaFactory.createValueMeta(TYPE_A));
    assertNotSame(byName, byType);

    assertEquals(NAME_A, ValueMetaFactory.getValueMetaName(TYPE_A));
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta(NAME_A));
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta("HOP9606 TYPE A"));
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta("hop9606 type a"));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("Hop9606 Type"));

    List<String> selectable = withName(BUILT_IN_SELECTABLE, NAME_A);
    assertEquals(selectable, Arrays.asList(ValueMetaFactory.getValueMetaNames()));
    List<String> all = withName(BUILT_IN_SELECTABLE, NAME_A);
    all.add(all.indexOf("String"), "Serializable");
    assertEquals(all, Arrays.asList(ValueMetaFactory.getAllValueMetaNames()));
    // "Hop9606 Type A" sorts between "Date" and "Integer"
    assertEquals("Date", selectable.get(selectable.indexOf(NAME_A) - 1));

    List<IValueMeta> classes = ValueMetaFactory.getValueMetaPluginClasses();
    assertEquals(BUILT_IN_NAMES_IN_ORDER.size() + 1, classes.size());
    assertSame(ValueMetaTestA.class, classes.get(5).getClass());
    assertSame(ValueMetaDate.class, classes.get(4).getClass());

    ValueMetaString source = new ValueMetaString("source");
    source.setLength(7);
    source.setComments("comments");
    IValueMeta converted = ValueMetaFactory.cloneValueMeta(source, TYPE_A);
    assertSame(ValueMetaTestA.class, converted.getClass());
    assertEquals("source", converted.getName());
    assertEquals(7, converted.getLength());
    assertEquals("comments", converted.getComments());

    IValueMeta cloned = ValueMetaFactory.cloneValueMeta(withLength);
    assertSame(ValueMetaTestA.class, cloned.getClass());
    assertNotSame(withLength, cloned);
    assertEquals("sized", cloned.getName());
    assertEquals(5, cloned.getLength());

    IValueMeta backToString = ValueMetaFactory.cloneValueMeta(withLength, IValueMeta.TYPE_STRING);
    assertSame(ValueMetaString.class, backToString.getClass());
    assertEquals("sized", backToString.getName());

    assertSame(ValueMetaString.class, ValueMetaFactory.createParseMeta(withLength).getClass());

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      withLength.writeMeta(out);
    }
    IValueMeta loaded;
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      loaded = ValueMetaFactory.loadValueMeta(in);
    }
    assertSame(ValueMetaTestA.class, loaded.getClass());
    assertEquals("sized", loaded.getName());
    assertEquals(5, loaded.getLength());

    IValueMeta fromJson = ValueMetaFactory.loadValueMetaFromJson(json("json", TYPE_A));
    assertSame(ValueMetaTestA.class, fromJson.getClass());
    assertEquals("json", fromJson.getName());
    assertEquals(3, fromJson.getLength());
  }

  @SuppressWarnings("unchecked")
  private static JSONObject json(String name, int type) {
    JSONObject json = new JSONObject();
    json.put("name", name);
    json.put("type", (long) type);
    json.put("length", 3L);
    json.put("precision", 1L);
    json.put("conversionMask", null);
    return json;
  }

  @Test
  void aTypeRegisteredThroughItsAnnotationIsUsableImmediately() throws Exception {
    assertUnknown(TYPE_ANNOTATED);

    PluginRegistry.getInstance()
        .registerPluginClass(
            ValueMetaAnnotated.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    IPlugin plugin =
        PluginRegistry.getInstance()
            .getPlugin(ValueMetaPluginType.class, String.valueOf(TYPE_ANNOTATED));
    assertNotNull(plugin);
    registered.add(plugin);

    IValueMeta valueMeta = ValueMetaFactory.createValueMeta("annotated", TYPE_ANNOTATED, 4, 0);
    assertSame(ValueMetaAnnotated.class, valueMeta.getClass());
    assertEquals("annotated", valueMeta.getName());
    assertEquals(4, valueMeta.getLength());
    assertEquals("Hop9606 Annotated", ValueMetaFactory.getValueMetaName(TYPE_ANNOTATED));
    assertEquals(TYPE_ANNOTATED, ValueMetaFactory.getIdForValueMeta("hop9606 annotated"));
    assertTrue(Arrays.asList(ValueMetaFactory.getValueMetaNames()).contains("Hop9606 Annotated"));

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, plugin);
    assertUnknown(TYPE_ANNOTATED);
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("Hop9606 Annotated"));
  }

  // Removing a type
  //

  @Test
  void aRemovedTypeFailsAgainEverywhere() throws Exception {
    IPlugin plugin = valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A));
    register(plugin);
    IValueMeta created = ValueMetaFactory.createValueMeta("created", TYPE_A, 5, 2);
    assertSame(ValueMetaTestA.class, created.getClass());
    assertEquals(NAME_A, ValueMetaFactory.getValueMetaName(TYPE_A));
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta(NAME_A));

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, plugin);

    assertUnknown(TYPE_A);
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(NAME_A));
    assertEquals(BUILT_IN_SELECTABLE, Arrays.asList(ValueMetaFactory.getValueMetaNames()));
    assertFalse(Arrays.asList(ValueMetaFactory.getAllValueMetaNames()).contains(NAME_A));
    assertEquals(
        BUILT_IN_NAMES_IN_ORDER.size(), ValueMetaFactory.getValueMetaPluginClasses().size());
    assertTrue(
        ValueMetaFactory.getValueMetaPluginClasses().stream()
            .noneMatch(ValueMetaTestA.class::isInstance));

    HopPluginException e =
        assertThrows(
            HopPluginException.class,
            () -> ValueMetaFactory.cloneValueMeta(new ValueMetaString("s"), TYPE_A));
    assertEquals(unknownTypeMessage(TYPE_A), e.getMessage());
    assertThrows(
        HopPluginException.class,
        () -> ValueMetaFactory.loadValueMetaFromJson(json("json", TYPE_A)));

    // Cloning an existing instance to its own type doesn't ask the registry
    IValueMeta cloned = ValueMetaFactory.cloneValueMeta(created);
    assertSame(ValueMetaTestA.class, cloned.getClass());
    assertEquals("created", cloned.getName());
    // ... and so does converting it to a type that is still registered
    assertSame(
        ValueMetaString.class,
        ValueMetaFactory.cloneValueMeta(created, IValueMeta.TYPE_STRING).getClass());
  }

  @Test
  void removingAndRegisteringAgainManyTimesAlwaysFollowsTheRegistry() throws Exception {
    IPlugin plugin = valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A));
    for (int i = 0; i < 5; i++) {
      register(plugin);
      assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
      assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta(NAME_A));
      PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, plugin);
      assertUnknown(TYPE_A);
      assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(NAME_A));
    }
  }

  // Replacing a type
  //

  @Test
  void reRegisteringTheSameNameAndIdWithAnotherClassReplacesIt() throws Exception {
    register(valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A)));
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());

    // Same name and ids: the registry replaces the plugin
    register(valueMetaPlugin(NAME_A, ValueMetaTestB.class, String.valueOf(TYPE_A)));
    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta("b", TYPE_A).getClass());
    assertSame(
        ValueMetaTestB.class, ValueMetaFactory.createValueMeta("b", TYPE_A, 1, 1).getClass());
    assertSame(
        ValueMetaTestB.class,
        ValueMetaFactory.cloneValueMeta(new ValueMetaString("s"), TYPE_A).getClass());
    assertEquals(
        1, Arrays.stream(ValueMetaFactory.getValueMetaNames()).filter(NAME_A::equals).count());

    // The name compares ignoring case, so this replaces it too
    register(valueMetaPlugin("HOP9606 TYPE A", ValueMetaTestC.class, String.valueOf(TYPE_A)));
    assertSame(ValueMetaTestC.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals("HOP9606 TYPE A", ValueMetaFactory.getValueMetaName(TYPE_A));
    assertEquals(BUILT_IN_NAMES_IN_ORDER.size() + 1, pluginCount());

    register(valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A)));
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals(NAME_A, ValueMetaFactory.getValueMetaName(TYPE_A));
  }

  @Test
  void registeringAgainAfterRemovalUsesTheNewClassAndName() throws Exception {
    IPlugin first = valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A));
    register(first);
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, first);
    assertUnknown(TYPE_A);

    register(valueMetaPlugin("Hop9606 Renamed", ValueMetaTestB.class, String.valueOf(TYPE_A)));

    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals("Hop9606 Renamed", ValueMetaFactory.getValueMetaName(TYPE_A));
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta("Hop9606 Renamed"));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(NAME_A));
  }

  @Test
  void ofTwoPluginsForTheSameIdTheFirstInNameOrderWinsNotTheFirstRegistered() throws Exception {
    IPlugin zulu = valueMetaPlugin("Hop9606 Zulu", ValueMetaTestA.class, String.valueOf(TYPE_A));
    register(zulu);
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals("Hop9606 Zulu", ValueMetaFactory.getValueMetaName(TYPE_A));

    // Registered later, but sorts first
    IPlugin alpha = valueMetaPlugin("Hop9606 Alpha", ValueMetaTestB.class, String.valueOf(TYPE_A));
    register(alpha);
    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals("Hop9606 Alpha", ValueMetaFactory.getValueMetaName(TYPE_A));
    // Both names lead to the id
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta("Hop9606 Alpha"));
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta("Hop9606 Zulu"));
    List<String> names = Arrays.asList(ValueMetaFactory.getValueMetaNames());
    assertTrue(names.indexOf("Hop9606 Alpha") >= 0);
    assertEquals(names.indexOf("Hop9606 Alpha") + 1, names.indexOf("Hop9606 Zulu"));

    // Registered last, sorts in between: doesn't change the winner
    IPlugin mike = valueMetaPlugin("Hop9606 Mike", ValueMetaTestC.class, String.valueOf(TYPE_A));
    register(mike);
    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, alpha);
    assertSame(ValueMetaTestC.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals("Hop9606 Mike", ValueMetaFactory.getValueMetaName(TYPE_A));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("Hop9606 Alpha"));

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, mike);
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals("Hop9606 Zulu", ValueMetaFactory.getValueMetaName(TYPE_A));

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, zulu);
    assertUnknown(TYPE_A);
  }

  @Test
  void aPluginWithSeveralIdsServesEachIdButIsNamedByTheFirst() throws Exception {
    IPlugin multi =
        valueMetaPlugin(
            "Hop9606 Multi",
            ValueMetaTestMulti.class,
            String.valueOf(TYPE_MULTI_MAIN),
            String.valueOf(TYPE_MULTI_SECOND));
    register(multi);

    IValueMeta main = ValueMetaFactory.createValueMeta("main", TYPE_MULTI_MAIN);
    IValueMeta second = ValueMetaFactory.createValueMeta("second", TYPE_MULTI_SECOND, 3, 1);
    assertSame(ValueMetaTestMulti.class, main.getClass());
    assertSame(ValueMetaTestMulti.class, second.getClass());
    assertNotSame(main, second);
    // The instance reports the type its class sets, not the id it was asked for
    assertEquals(TYPE_MULTI_MAIN, second.getType());
    assertEquals("second", second.getName());
    assertEquals(3, second.getLength());

    assertEquals("Hop9606 Multi", ValueMetaFactory.getValueMetaName(TYPE_MULTI_MAIN));
    assertEquals("-", ValueMetaFactory.getValueMetaName(TYPE_MULTI_SECOND));
    assertEquals(TYPE_MULTI_MAIN, ValueMetaFactory.getIdForValueMeta("Hop9606 Multi"));
    assertEquals(
        1,
        Arrays.stream(ValueMetaFactory.getValueMetaNames())
            .filter("Hop9606 Multi"::equals)
            .count());
    assertEquals(
        1,
        ValueMetaFactory.getValueMetaPluginClasses().stream()
            .filter(ValueMetaTestMulti.class::isInstance)
            .count());

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, multi);
    assertUnknown(TYPE_MULTI_MAIN);
    assertUnknown(TYPE_MULTI_SECOND);
  }

  @Test
  void aSecondaryIdShadowsABuiltInTypeOnlyWhenThePluginSortsFirst() throws Exception {
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());

    // "Hop9606 Shadow" sorts before "String" and also claims id 2
    IPlugin before = valueMetaPlugin("Hop9606 Shadow", ValueMetaTestA.class, "9606010", "2");
    register(before);
    assertSame(
        ValueMetaTestA.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    assertSame(
        ValueMetaTestA.class,
        ValueMetaFactory.cloneValueMeta(new ValueMetaInteger("i"), IValueMeta.TYPE_STRING)
            .getClass());
    // Names only follow the first id
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("String"));
    assertEquals(9606010, ValueMetaFactory.getIdForValueMeta("Hop9606 Shadow"));

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, before);
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());

    // "Zzz Hop9606 Shadow" sorts after "String": String keeps id 2
    IPlugin after = valueMetaPlugin("Zzz Hop9606 Shadow", ValueMetaTestA.class, "9606011", "2");
    register(after);
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(9606011).getClass());
  }

  @Test
  void aPluginWithTheMainIdOfABuiltInTypeTakesOverWhenItSortsFirst() throws Exception {
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));

    IPlugin text = valueMetaPlugin("Hop9606 Text", ValueMetaTestB.class, "2");
    register(text);

    assertSame(
        ValueMetaTestB.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    assertEquals("Hop9606 Text", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("String"));
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("Hop9606 Text"));
    List<String> names = Arrays.asList(ValueMetaFactory.getValueMetaNames());
    assertTrue(names.contains("Hop9606 Text"));
    assertTrue(names.contains("String"));

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, text);
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
  }

  @Test
  void ofTwoPluginsWithTheSameNameTheIdsDecideWhichOneANameLookupFinds() throws Exception {
    // Registered first, but its id sorts last
    register(valueMetaPlugin("Hop9606 Same", ValueMetaTestB.class, "9606021"));
    assertEquals(9606021, ValueMetaFactory.getIdForValueMeta("Hop9606 Same"));

    IPlugin lower = valueMetaPlugin("Hop9606 Same", ValueMetaTestA.class, "9606020");
    register(lower);
    assertEquals(9606020, ValueMetaFactory.getIdForValueMeta("Hop9606 Same"));
    assertEquals(9606020, ValueMetaFactory.getIdForValueMeta("HOP9606 SAME"));
    assertEquals("Hop9606 Same", ValueMetaFactory.getValueMetaName(9606020));
    assertEquals("Hop9606 Same", ValueMetaFactory.getValueMetaName(9606021));
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(9606020).getClass());
    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(9606021).getClass());
    assertEquals(
        2,
        Arrays.stream(ValueMetaFactory.getValueMetaNames()).filter("Hop9606 Same"::equals).count());

    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, lower);
    assertEquals(9606021, ValueMetaFactory.getIdForValueMeta("Hop9606 Same"));
    assertUnknown(9606020);
  }

  @Test
  void namesDifferingOnlyInCaseAreOrderedByTheirIds() throws Exception {
    register(valueMetaPlugin("HOP9606 CASE", ValueMetaTestB.class, "9606031"));
    register(valueMetaPlugin("hop9606 case", ValueMetaTestA.class, "9606030"));

    assertEquals(9606030, ValueMetaFactory.getIdForValueMeta("Hop9606 Case"));
    assertEquals(9606030, ValueMetaFactory.getIdForValueMeta("HOP9606 CASE"));
    assertEquals("HOP9606 CASE", ValueMetaFactory.getValueMetaName(9606031));
    assertEquals("hop9606 case", ValueMetaFactory.getValueMetaName(9606030));
    List<String> names = Arrays.asList(ValueMetaFactory.getValueMetaNames());
    assertEquals(names.indexOf("hop9606 case") + 1, names.indexOf("HOP9606 CASE"));
  }

  @Test
  void ofTwoPluginsWithTheSameNameTheOneWithFewerIdsSortsFirst() throws Exception {
    register(valueMetaPlugin("Hop9606 Same", ValueMetaTestA.class, "9606022", "9606023"));
    register(valueMetaPlugin("Hop9606 Same", ValueMetaTestB.class, "9606029"));

    assertEquals(9606029, ValueMetaFactory.getIdForValueMeta("Hop9606 Same"));
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(9606023).getClass());
    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(9606029).getClass());
  }

  // Unusual ids and names
  //

  @Test
  void anIdWithALeadingZeroIsFoundByNameButNotByNumber() throws Exception {
    register(valueMetaPlugin("Hop9606 Padded", ValueMetaTestA.class, "09606040"));

    assertUnknown(9606040);
    assertEquals(9606040, ValueMetaFactory.getIdForValueMeta("Hop9606 Padded"));
    assertTrue(Arrays.asList(ValueMetaFactory.getValueMetaNames()).contains("Hop9606 Padded"));
  }

  @Test
  void aNegativeIdIsCreatableButNotSelectable() throws Exception {
    register(valueMetaPlugin("Hop9606 Negative", ValueMetaTestA.class, "-9606041"));

    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(-9606041).getClass());
    assertEquals("Hop9606 Negative", ValueMetaFactory.getValueMetaName(-9606041));
    assertEquals(-9606041, ValueMetaFactory.getIdForValueMeta("Hop9606 Negative"));
    assertFalse(Arrays.asList(ValueMetaFactory.getValueMetaNames()).contains("Hop9606 Negative"));
    assertTrue(Arrays.asList(ValueMetaFactory.getAllValueMetaNames()).contains("Hop9606 Negative"));
  }

  @Test
  void aPluginWithoutANameIsListedFirstAndNamedNull() throws Exception {
    register(valueMetaPlugin(null, ValueMetaTestA.class, "9606042"));

    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(9606042).getClass());
    assertNull(ValueMetaFactory.getValueMetaName(9606042));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(null));
    // Plugin.nullStringComparator means "nulls last", but CompareToBuilder handles a null before
    // the comparator sees it and sorts it first.
    String[] names = ValueMetaFactory.getValueMetaNames();
    assertNull(names[0]);
    assertEquals("Avro Record", names[1]);
    assertEquals(BUILT_IN_SELECTABLE.size() + 1, names.length);
  }

  @Test
  void aNonNumericIdBreaksTheNameListsThatParseIt() throws Exception {
    register(valueMetaPlugin("Hop9606 Text Id", ValueMetaTestA.class, "hop9606"));

    assertThrows(NumberFormatException.class, ValueMetaFactory::getValueMetaNames);
    assertThrows(
        NumberFormatException.class, () -> ValueMetaFactory.getIdForValueMeta("Hop9606 Text Id"));
    assertTrue(Arrays.asList(ValueMetaFactory.getAllValueMetaNames()).contains("Hop9606 Text Id"));
    // Other lookups are not affected
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("String"));
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
  }

  // Classes that can't be created
  //

  @Test
  void aClassThatDoesNotExistFailsOnEveryCall() throws Exception {
    Map<Class<?>, String> classMap = new HashMap<>();
    classMap.put(IValueMeta.class, "org.apache.hop.core.row.value.DoesNotExist9606");
    register(valueMetaPlugin("Hop9606 Missing", classMap, String.valueOf(TYPE_A)));

    for (int i = 0; i < 3; i++) {
      HopPluginException e =
          assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));
      assertSame(HopPluginException.class, e.getClass());
      assertInstanceOf(ClassNotFoundException.class, e.getCause());
      assertTrue(e.getMessage().contains("Class not found"), e.getMessage());
    }
    assertEquals("Hop9606 Missing", ValueMetaFactory.getValueMetaName(TYPE_A));
  }

  @Test
  void aPluginWithoutAClassForValueMetaFailsWithAClassMapException() throws Exception {
    register(valueMetaPlugin("Hop9606 Empty", new HashMap<>(), String.valueOf(TYPE_A)));

    for (int i = 0; i < 2; i++) {
      HopPluginClassMapException e =
          assertThrows(
              HopPluginClassMapException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));
      assertTrue(
          e.getMessage()
              .contains(
                  "Plugin \"Hop9606 Empty\" is unable to load class " + IValueMeta.class.getName()),
          e.getMessage());
    }
  }

  @Test
  void aClassWithoutAnAccessibleConstructorFailsOnEveryCall() throws Exception {
    register(
        valueMetaPlugin(
            "Hop9606 Private", ValueMetaPrivateConstructor.class, String.valueOf(TYPE_A)));

    for (int i = 0; i < 3; i++) {
      HopPluginException e =
          assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));
      assertInstanceOf(IllegalAccessException.class, e.getCause());
      assertTrue(e.getMessage().contains("Illegal access to class"), e.getMessage());
    }
  }

  @Test
  void anAbstractClassFailsOnEveryCall() throws Exception {
    register(valueMetaPlugin("Hop9606 Abstract", ValueMetaAbstract.class, String.valueOf(TYPE_A)));

    for (int i = 0; i < 3; i++) {
      HopPluginException e =
          assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));
      assertInstanceOf(InstantiationException.class, e.getCause());
      assertTrue(e.getMessage().contains("Unable to instantiate class"), e.getMessage());
    }
  }

  @Test
  void eachCreateValueMetaRunsTheConstructorExactlyOnce() throws Exception {
    register(
        valueMetaPlugin(
            "Hop9606 Counting", ValueMetaCounting.class, String.valueOf(TYPE_COUNTING)));
    ValueMetaCounting.CONSTRUCTED.set(0);

    for (int i = 1; i <= 5; i++) {
      ValueMetaFactory.createValueMeta("c", TYPE_COUNTING);
      assertEquals(i, ValueMetaCounting.CONSTRUCTED.get());
    }
    // Name lookups don't create anything
    ValueMetaFactory.getValueMetaName(TYPE_COUNTING);
    ValueMetaFactory.getIdForValueMeta("Hop9606 Counting");
    ValueMetaFactory.getValueMetaNames();
    assertEquals(5, ValueMetaCounting.CONSTRUCTED.get());
  }

  @Test
  void aConstructorThatThrowsFailsTheCallUntilItStopsThrowing() throws Exception {
    register(
        valueMetaPlugin(
            "Hop9606 Counting", ValueMetaCounting.class, String.valueOf(TYPE_COUNTING)));
    assertSame(ValueMetaCounting.class, ValueMetaFactory.createValueMeta(TYPE_COUNTING).getClass());

    ValueMetaCounting.failing = true;
    for (int i = 0; i < 2; i++) {
      HopPluginException e =
          assertThrows(
              HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_COUNTING));
      assertSame(HopPluginException.class, e.getClass());
      assertTrue(e.getMessage().contains("Unexpected error loading class"), e.getMessage());
      assertInstanceOf(InvocationTargetException.class, e.getCause());
      assertInstanceOf(IllegalStateException.class, e.getCause().getCause());
    }

    ValueMetaCounting.failing = false;
    assertSame(ValueMetaCounting.class, ValueMetaFactory.createValueMeta(TYPE_COUNTING).getClass());
  }

  @Test
  void aConstructorThatThrowsRunsOncePerFailingCall() throws Exception {
    register(
        valueMetaPlugin(
            "Hop9606 Counting", ValueMetaCounting.class, String.valueOf(TYPE_COUNTING)));
    ValueMetaFactory.createValueMeta(TYPE_COUNTING);
    ValueMetaFactory.createValueMeta(TYPE_COUNTING);
    ValueMetaCounting.CONSTRUCTED.set(0);

    ValueMetaCounting.failing = true;
    assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_COUNTING));

    assertEquals(1, ValueMetaCounting.CONSTRUCTED.get());
  }

  // Plugins that load their own classes
  //

  @Test
  void aClassLoadingPluginIsAskedOnEveryCall() throws Exception {
    CountingClassLoadingPlugin plugin =
        new CountingClassLoadingPlugin(
            "Hop9606 Loader", ValueMetaTestA::new, String.valueOf(TYPE_CLASS_LOADING));
    register(plugin);

    IValueMeta first = ValueMetaFactory.createValueMeta(TYPE_CLASS_LOADING);
    IValueMeta second = ValueMetaFactory.createValueMeta("second", TYPE_CLASS_LOADING);
    IValueMeta third = ValueMetaFactory.createValueMeta("third", TYPE_CLASS_LOADING, 4, 2);
    assertEquals(3, plugin.loadClassCalls.get());
    assertSame(ValueMetaTestA.class, first.getClass());
    assertNotSame(first, second);
    assertEquals("third", third.getName());
    assertEquals(4, third.getLength());

    for (int i = 0; i < 10; i++) {
      ValueMetaFactory.createValueMeta(TYPE_CLASS_LOADING);
    }
    assertEquals(13, plugin.loadClassCalls.get());

    ValueMetaFactory.cloneValueMeta(new ValueMetaString("s"), TYPE_CLASS_LOADING);
    assertEquals(14, plugin.loadClassCalls.get());

    // getValueMetaPluginClasses() asks for the main type of every plugin
    assertEquals(
        1,
        ValueMetaFactory.getValueMetaPluginClasses().stream()
            .filter(ValueMetaTestA.class::isInstance)
            .count());
    assertEquals(15, plugin.loadClassCalls.get());

    // Name lookups don't load anything
    assertEquals("Hop9606 Loader", ValueMetaFactory.getValueMetaName(TYPE_CLASS_LOADING));
    assertEquals(TYPE_CLASS_LOADING, ValueMetaFactory.getIdForValueMeta("hop9606 loader"));
    assertTrue(Arrays.asList(ValueMetaFactory.getValueMetaNames()).contains("Hop9606 Loader"));
    assertEquals(15, plugin.loadClassCalls.get());
  }

  @Test
  void aClassLoadingPluginThatHandsOutOneInstanceGetsThatInstanceBackEveryTime() throws Exception {
    ValueMetaTestA shared = new ValueMetaTestA();
    CountingClassLoadingPlugin plugin =
        new CountingClassLoadingPlugin(
            "Hop9606 Loader", () -> shared, String.valueOf(TYPE_CLASS_LOADING));
    register(plugin);

    IValueMeta first = ValueMetaFactory.createValueMeta("one", TYPE_CLASS_LOADING);
    assertSame(shared, first);
    assertEquals("one", shared.getName());

    IValueMeta second = ValueMetaFactory.createValueMeta("two", TYPE_CLASS_LOADING, 3, 1);
    assertSame(shared, second);
    assertEquals("two", shared.getName());
    assertEquals(3, shared.getLength());
  }

  @Test
  void aClassLoadingPluginCanHandOutAnotherClassOnTheNextCall() throws Exception {
    CountingClassLoadingPlugin plugin =
        new CountingClassLoadingPlugin(
            "Hop9606 Loader", ValueMetaTestA::new, String.valueOf(TYPE_CLASS_LOADING));
    register(plugin);
    assertSame(
        ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_CLASS_LOADING).getClass());
    assertSame(
        ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_CLASS_LOADING).getClass());

    plugin.setSupplier(ValueMetaTestB::new);

    assertSame(
        ValueMetaTestB.class, ValueMetaFactory.createValueMeta(TYPE_CLASS_LOADING).getClass());
  }

  @Test
  void aClassLoadingPluginThatLoadsNothingFailsOnEveryCall() throws Exception {
    CountingClassLoadingPlugin plugin =
        new CountingClassLoadingPlugin(
            "Hop9606 Loader", () -> null, String.valueOf(TYPE_CLASS_LOADING));
    register(plugin);

    for (int i = 1; i <= 3; i++) {
      HopPluginClassMapException e =
          assertThrows(
              HopPluginClassMapException.class,
              () -> ValueMetaFactory.createValueMeta(TYPE_CLASS_LOADING));
      assertTrue(
          e.getMessage()
              .contains(
                  "Plugin \"Hop9606 Loader\" is unable to load class "
                      + IValueMeta.class.getName()),
          e.getMessage());
      assertEquals(i, plugin.loadClassCalls.get());
    }

    plugin.setSupplier(ValueMetaTestA::new);
    assertSame(
        ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_CLASS_LOADING).getClass());
  }

  @Test
  void aClassFactoryAddedToThePluginRegistryIsAskedOnEveryCall() throws Exception {
    // A plain plugin without a class of its own: the registry falls back to a class factory
    register(valueMetaPlugin("Hop9606 Factory", new HashMap<>(), String.valueOf(TYPE_FACTORY)));
    AtomicInteger calls = new AtomicInteger();
    PluginRegistry.getInstance()
        .addClassFactory(
            ValueMetaPluginType.class,
            IValueMeta.class,
            String.valueOf(TYPE_FACTORY),
            () -> {
              IValueMeta valueMeta = new ValueMetaTestA();
              valueMeta.setComments("from factory " + calls.incrementAndGet());
              return valueMeta;
            });

    for (int i = 1; i <= 3; i++) {
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta("f" + i, TYPE_FACTORY);
      assertSame(ValueMetaTestA.class, valueMeta.getClass());
      assertEquals("f" + i, valueMeta.getName());
      assertEquals(i, calls.get());
      assertEquals("from factory " + i, valueMeta.getComments());
    }
  }

  // Changes outside the registry
  //

  @Test
  void aClassMapChangedInPlaceIsSeenByTheNextCall() throws Exception {
    Map<Class<?>, String> classMap = new HashMap<>();
    classMap.put(IValueMeta.class, ValueMetaTestA.class.getName());
    IPlugin plugin = valueMetaPlugin(NAME_A, classMap, String.valueOf(TYPE_A));
    register(plugin);
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());

    // What IPlugin.merge() does with the class map of a plugin fragment
    plugin.getClassMap().put(IValueMeta.class, ValueMetaTestB.class.getName());

    assertSame(ValueMetaTestB.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
  }

  // Resetting the registry
  //

  @Test
  void aResetRegistryHasNoTypesUntilTheEnvironmentIsInitializedAgain() throws Exception {
    register(valueMetaPlugin(NAME_A, ValueMetaTestA.class, String.valueOf(TYPE_A)));
    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    try {
      HopClientEnvironment.reset();

      assertUnknown(IValueMeta.TYPE_STRING);
      assertUnknown(TYPE_A);
      assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("String"));
      assertArrayEquals(new String[0], ValueMetaFactory.getValueMetaNames());
      assertArrayEquals(new String[0], ValueMetaFactory.getAllValueMetaNames());
      assertTrue(ValueMetaFactory.getValueMetaPluginClasses().isEmpty());
    } finally {
      HopClientEnvironment.init();
    }

    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("string"));
    assertEquals(BUILT_IN_SELECTABLE, Arrays.asList(ValueMetaFactory.getValueMetaNames()));
    // The plugin registered at runtime is gone with the reset
    assertUnknown(TYPE_A);
  }

  private static int pluginCount() {
    return PluginRegistry.getInstance().getPlugins(ValueMetaPluginType.class).size();
  }
}
