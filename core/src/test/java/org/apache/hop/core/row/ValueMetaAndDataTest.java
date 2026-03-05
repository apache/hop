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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

class ValueMetaAndDataTest {
  private PluginRegistry pluginRegistry;

  @BeforeEach
  void before() {
    pluginRegistry = mock(PluginRegistry.class);
  }

  @Test
  void testConstructors() {
    ValueMetaAndData result;

    result = new ValueMetaAndData(new ValueMetaString("ValueStringName"), "testValue1");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_STRING, result.getValueMeta().getType());
    assertEquals("ValueStringName", result.getValueMeta().getName());
    assertEquals("testValue1", result.getValueData());

    result = new ValueMetaAndData("StringName", "testValue2");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_STRING, result.getValueMeta().getType());
    assertEquals("StringName", result.getValueMeta().getName());
    assertEquals("testValue2", result.getValueData());

    result = new ValueMetaAndData("NumberName", Double.valueOf("123.45"));
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_NUMBER, result.getValueMeta().getType());
    assertEquals("NumberName", result.getValueMeta().getName());
    assertEquals(Double.valueOf("123.45"), result.getValueData());

    result = new ValueMetaAndData("IntegerName", 234L);
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_INTEGER, result.getValueMeta().getType());
    assertEquals("IntegerName", result.getValueMeta().getName());
    assertEquals(234L, result.getValueData());

    Date testDate = Calendar.getInstance().getTime();
    result = new ValueMetaAndData("DateName", testDate);
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_DATE, result.getValueMeta().getType());
    assertEquals("DateName", result.getValueMeta().getName());
    assertEquals(testDate, result.getValueData());

    result = new ValueMetaAndData("BigNumberName", new BigDecimal("123456789.987654321"));
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_BIGNUMBER, result.getValueMeta().getType());
    assertEquals("BigNumberName", result.getValueMeta().getName());
    assertEquals(new BigDecimal("123456789.987654321"), result.getValueData());

    testConstructors_other(result);
  }

  private void testConstructors_other(ValueMetaAndData result) {
    result = new ValueMetaAndData("BooleanName", Boolean.TRUE);
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_BOOLEAN, result.getValueMeta().getType());
    assertEquals("BooleanName", result.getValueMeta().getName());
    assertEquals(Boolean.TRUE, result.getValueData());

    byte[] testBytes = new byte[50];
    new Random().nextBytes(testBytes);
    result = new ValueMetaAndData("BinaryName", testBytes);
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_BINARY, result.getValueMeta().getType());
    assertEquals("BinaryName", result.getValueMeta().getName());
    assertArrayEquals(testBytes, (byte[]) result.getValueData());

    result = new ValueMetaAndData("SerializableName", new StringBuilder("serializable test"));
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_SERIALIZABLE, result.getValueMeta().getType());
    assertEquals("SerializableName", result.getValueMeta().getName());
    assertInstanceOf(StringBuilder.class, result.getValueData());
    assertEquals("serializable test", result.getValueData().toString());
  }

  @Test
  void testLoadXml() {
    List<IPlugin> pluginTypeList = new ArrayList<>();
    IPlugin plugin = mock(IPlugin.class);
    when(plugin.getName()).thenReturn("3");
    String[] ids = {"3"};
    when(plugin.getIds()).thenReturn(ids);
    pluginTypeList.add(plugin);
    when(pluginRegistry.getPlugins(ValueMetaPluginType.class)).thenReturn(pluginTypeList);
    ValueMetaFactory.pluginRegistry = pluginRegistry;

    NodeList nodeList = mock(NodeList.class);
    when(nodeList.getLength()).thenReturn(2);
    Node node = mock(Node.class);
    when(node.getChildNodes()).thenReturn(nodeList);

    Node childNodeText = mock(Node.class);
    when(childNodeText.getNodeName()).thenReturn("text");
    when(nodeList.item(0)).thenReturn(childNodeText);
    Node nodeValue = mock(Node.class);
    when(childNodeText.getFirstChild()).thenReturn(nodeValue);
    String testData = "2010/01/01 00:00:00.000";
    when(nodeValue.getNodeValue()).thenReturn(testData);

    Node childNodeType = mock(Node.class);
    when(childNodeType.getNodeName()).thenReturn("type");
    when(nodeList.item(1)).thenReturn(childNodeType);
    Node nodeTypeValue = mock(Node.class);
    when(childNodeType.getFirstChild()).thenReturn(nodeTypeValue);
    when(nodeTypeValue.getNodeValue()).thenReturn("3");

    String value = nodeTypeValue.getNodeValue();
    assertNotNull(value);
    verify(nodeTypeValue).getNodeValue();
  }
}
