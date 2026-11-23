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

import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.TYPE_A;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.unknownTypeMessage;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.valueMetaPlugin;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.ValueMetaTestA;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Pins that {@link ValueMetaFactory} asks whatever registry {@link ValueMetaFactory#pluginRegistry}
 * holds, on every call, when a test replaces it with a double; and that it follows the real
 * registry again once it is put back.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaFactoryRegistryDoubleTest {

  private PluginRegistry registryDouble;
  private final List<IPlugin> registered = new ArrayList<>();

  @BeforeEach
  void installDouble() throws Exception {
    // Use the real registry first, so anything the factory remembers about it is filled in
    assertSame(PluginRegistry.getInstance(), ValueMetaFactory.pluginRegistry);
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("String"));

    registryDouble = mock(PluginRegistry.class);
    ValueMetaFactory.pluginRegistry = registryDouble;
  }

  @AfterEach
  void restoreRealRegistry() {
    ValueMetaFactory.pluginRegistry = PluginRegistry.getInstance();
    for (IPlugin plugin : registered) {
      PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, plugin);
    }
  }

  private static IPlugin pluginDouble(String name, String... ids) {
    IPlugin plugin = mock(IPlugin.class);
    when(plugin.getIds()).thenReturn(ids);
    when(plugin.getName()).thenReturn(name);
    for (String id : ids) {
      when(plugin.matches(id)).thenReturn(true);
    }
    return plugin;
  }

  @Test
  void createValueMetaAsksTheDoubleOnEveryCall() throws Exception {
    IPlugin plugin = pluginDouble("String", "2");
    when(registryDouble.getPlugin(ValueMetaPluginType.class, "2")).thenReturn(plugin);
    when(registryDouble.loadClass(plugin, IValueMeta.class))
        .thenAnswer(invocation -> new ValueMetaString());

    IValueMeta first = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING);
    IValueMeta second = ValueMetaFactory.createValueMeta("second", IValueMeta.TYPE_STRING);
    IValueMeta third = ValueMetaFactory.createValueMeta("third", IValueMeta.TYPE_STRING, 5, 1);

    verify(registryDouble, times(3)).getPlugin(ValueMetaPluginType.class, "2");
    verify(registryDouble, times(3)).loadClass(plugin, IValueMeta.class);
    assertNotSame(first, second);
    assertEquals("second", second.getName());
    assertEquals("third", third.getName());
    assertEquals(5, third.getLength());
  }

  @Test
  void createValueMetaReturnsWhatTheDoubleLoadsEvenTheSameInstance() throws Exception {
    IPlugin plugin = pluginDouble("String", "2");
    ValueMetaInteger shared = new ValueMetaInteger();
    when(registryDouble.getPlugin(ValueMetaPluginType.class, "2")).thenReturn(plugin);
    when(registryDouble.loadClass(plugin, IValueMeta.class)).thenReturn(shared);

    assertSame(shared, ValueMetaFactory.createValueMeta("one", IValueMeta.TYPE_STRING));
    assertEquals("one", shared.getName());
    assertSame(shared, ValueMetaFactory.createValueMeta("two", IValueMeta.TYPE_STRING, 4, 0));
    assertEquals("two", shared.getName());
    assertEquals(4, shared.getLength());

    // The answer may change from one call to the next
    when(registryDouble.loadClass(plugin, IValueMeta.class))
        .thenAnswer(invocation -> new ValueMetaDate());
    assertSame(
        ValueMetaDate.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
  }

  @Test
  void createValueMetaFailsWhenTheDoubleKnowsNoPluginEvenForABuiltInType() throws Exception {
    when(registryDouble.getPlugin(any(), anyString())).thenReturn(null);

    for (int i = 0; i < 2; i++) {
      HopPluginException e =
          assertThrows(
              HopPluginException.class,
              () -> ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING));
      assertSame(HopPluginException.class, e.getClass());
      assertEquals(unknownTypeMessage(IValueMeta.TYPE_STRING), e.getMessage());
    }
    verify(registryDouble, times(2)).getPlugin(ValueMetaPluginType.class, "2");
    verify(registryDouble, never()).loadClass(any(IPlugin.class), eq(IValueMeta.class));
  }

  @Test
  void createValueMetaUsesTheDoubleForATypeTheRealRegistryDoesNotHave() throws Exception {
    IPlugin plugin = pluginDouble("Hop9606 Double", String.valueOf(TYPE_A));
    when(registryDouble.getPlugin(ValueMetaPluginType.class, String.valueOf(TYPE_A)))
        .thenReturn(plugin);
    when(registryDouble.loadClass(plugin, IValueMeta.class))
        .thenAnswer(invocation -> new ValueMetaTestA());

    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertSame(
        ValueMetaTestA.class,
        ValueMetaFactory.cloneValueMeta(new ValueMetaString("s"), TYPE_A).getClass());
  }

  @Test
  void nameLookupsAskTheDoubleOnEveryCall() throws Exception {
    IPlugin text = pluginDouble("Text", "2");
    IPlugin whole = pluginDouble("Whole", "5");
    when(registryDouble.getPlugins(ValueMetaPluginType.class)).thenReturn(List.of(text, whole));

    assertEquals("Text", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals("-", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_DATE));
    assertEquals(IValueMeta.TYPE_INTEGER, ValueMetaFactory.getIdForValueMeta("WHOLE"));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("String"));
    assertArrayEquals(new String[] {"Text", "Whole"}, ValueMetaFactory.getValueMetaNames());
    assertArrayEquals(new String[] {"Text", "Whole"}, ValueMetaFactory.getAllValueMetaNames());
    verify(registryDouble, times(6)).getPlugins(ValueMetaPluginType.class);

    // The double now answers something else: so does the factory
    IPlugin chars = pluginDouble("Chars", "2");
    when(registryDouble.getPlugins(ValueMetaPluginType.class)).thenReturn(List.of(chars));

    assertEquals("Chars", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("Whole"));
    assertEquals(IValueMeta.TYPE_STRING, ValueMetaFactory.getIdForValueMeta("chars"));
    assertArrayEquals(new String[] {"Chars"}, ValueMetaFactory.getValueMetaNames());
    assertArrayEquals(new String[] {"Chars"}, ValueMetaFactory.getAllValueMetaNames());
    verify(registryDouble, times(11)).getPlugins(ValueMetaPluginType.class);
  }

  @Test
  void getValueMetaPluginClassesLoadsEveryPluginOfTheDouble() throws Exception {
    IPlugin text = pluginDouble("Text", "2");
    IPlugin whole = pluginDouble("Whole", "5");
    when(registryDouble.getPlugins(ValueMetaPluginType.class)).thenReturn(List.of(text, whole));
    when(registryDouble.loadClass(text)).thenAnswer(invocation -> new ValueMetaString());
    when(registryDouble.loadClass(whole)).thenAnswer(invocation -> new ValueMetaInteger());

    List<IValueMeta> classes = ValueMetaFactory.getValueMetaPluginClasses();
    assertEquals(2, classes.size());
    assertSame(ValueMetaString.class, classes.get(0).getClass());
    assertSame(ValueMetaInteger.class, classes.get(1).getClass());
    ValueMetaFactory.getValueMetaPluginClasses();
    verify(registryDouble, times(2)).loadClass(text);
    verify(registryDouble, times(2)).loadClass(whole);
  }

  @Test
  void anEmptyDoubleKnowsNoTypes() throws Exception {
    when(registryDouble.getPlugins(ValueMetaPluginType.class)).thenReturn(List.of());

    assertEquals("-", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta("String"));
    assertArrayEquals(new String[0], ValueMetaFactory.getValueMetaNames());
    assertArrayEquals(new String[0], ValueMetaFactory.getAllValueMetaNames());
    assertTrue(ValueMetaFactory.getValueMetaPluginClasses().isEmpty());
  }

  @Test
  void theRealRegistryIsFollowedAgainOnceItIsPutBack() throws Exception {
    when(registryDouble.getPlugin(any(), anyString())).thenReturn(null);
    when(registryDouble.getPlugins(ValueMetaPluginType.class)).thenReturn(List.of());
    assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));

    // The real registry changes while the double is installed
    IPlugin plugin = valueMetaPlugin("Hop9606 Real", ValueMetaTestA.class, String.valueOf(TYPE_A));
    registered.add(plugin);
    PluginRegistry.getInstance().registerPlugin(ValueMetaPluginType.class, plugin);

    // ... which the factory doesn't see yet
    assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));
    assertEquals("-", ValueMetaFactory.getValueMetaName(TYPE_A));
    assertThrows(
        HopPluginException.class, () -> ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING));

    ValueMetaFactory.pluginRegistry = PluginRegistry.getInstance();

    assertSame(ValueMetaTestA.class, ValueMetaFactory.createValueMeta(TYPE_A).getClass());
    assertEquals("Hop9606 Real", ValueMetaFactory.getValueMetaName(TYPE_A));
    assertEquals(TYPE_A, ValueMetaFactory.getIdForValueMeta("Hop9606 Real"));
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    assertEquals("String", ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));

    // ... and its removal too
    PluginRegistry.getInstance().removePlugin(ValueMetaPluginType.class, plugin);
    HopPluginException e =
        assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(TYPE_A));
    assertEquals(unknownTypeMessage(TYPE_A), e.getMessage());
  }

  @Test
  void aSecondDoubleReplacesTheFirst() throws Exception {
    IPlugin plugin = pluginDouble("String", "2");
    when(registryDouble.getPlugin(ValueMetaPluginType.class, "2")).thenReturn(plugin);
    when(registryDouble.loadClass(plugin, IValueMeta.class))
        .thenAnswer(invocation -> new ValueMetaString());
    assertSame(
        ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());

    PluginRegistry secondDouble = mock(PluginRegistry.class);
    when(secondDouble.getPlugin(ValueMetaPluginType.class, "2")).thenReturn(plugin);
    when(secondDouble.loadClass(plugin, IValueMeta.class))
        .thenAnswer(invocation -> new ValueMetaBinary());
    ValueMetaFactory.pluginRegistry = secondDouble;

    assertSame(
        ValueMetaBinary.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING).getClass());
    verify(registryDouble, times(1)).getPlugin(ValueMetaPluginType.class, "2");
    verify(secondDouble, times(1)).getPlugin(ValueMetaPluginType.class, "2");
  }
}
