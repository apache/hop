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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class RowMetaTest {
  IRowMeta rowMeta = new RowMeta();
  IValueMeta string;
  IValueMeta integer;
  IValueMeta date;

  IValueMeta charly;
  IValueMeta dup;
  IValueMeta bin;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    string = ValueMetaFactory.createValueMeta("string", IValueMeta.TYPE_STRING);
    rowMeta.addValueMeta(string);
    integer = ValueMetaFactory.createValueMeta("integer", IValueMeta.TYPE_INTEGER);
    rowMeta.addValueMeta(integer);
    date = ValueMetaFactory.createValueMeta("date", IValueMeta.TYPE_DATE);
    rowMeta.addValueMeta(date);

    charly = ValueMetaFactory.createValueMeta("charly", IValueMeta.TYPE_SERIALIZABLE);

    dup = ValueMetaFactory.createValueMeta("dup", IValueMeta.TYPE_SERIALIZABLE);
    bin = ValueMetaFactory.createValueMeta("bin", IValueMeta.TYPE_BINARY);
  }

  private List<IValueMeta> generateVList(String[] names, int[] types) throws HopPluginException {
    List<IValueMeta> list = new ArrayList<>();
    for (int i = 0; i < names.length; i++) {
      IValueMeta vm = ValueMetaFactory.createValueMeta(names[i], types[i]);
      vm.setOrigin("originTransform");
      list.add(vm);
    }
    return list;
  }

  @Test
  void testRowMetaInitializingFromXmlNode() throws Exception {
    String testXmlNode = null;
    try (InputStream in = RowMetaTest.class.getResourceAsStream("rowMetaNode.xml")) {
      testXmlNode = IOUtils.toString(in, StandardCharsets.UTF_8);
    }
    Document xmlDoc = XmlHandler.loadXmlString(testXmlNode);
    RowMeta meta = spy(new RowMeta(XmlHandler.getSubNode(xmlDoc, RowMeta.XML_META_TAG)));
    assertEquals(2, meta.getValueMetaList().size());
    IValueMeta valueMeta = meta.getValueMeta(0);
    assertInstanceOf(ValueMetaDate.class, valueMeta);
    assertEquals("testDate", valueMeta.getName());
    assertNull(valueMeta.getConversionMask());
    valueMeta = meta.getValueMeta(1);
    assertInstanceOf(ValueMetaTimestamp.class, valueMeta);
    assertEquals("testTimestamp", valueMeta.getName());
    assertEquals("yyyy/MM/dd HH:mm:ss.000000000", valueMeta.getConversionMask());
  }

  @Test
  void testGetValueMetaList() {
    List<IValueMeta> list = rowMeta.getValueMetaList();
    assertTrue(list.contains(string));
    assertTrue(list.contains(integer));
    assertTrue(list.contains(date));
  }

  @Test
  void testSetValueMetaList() throws HopPluginException {
    List<IValueMeta> setList =
        this.generateVList(new String[] {"alpha", "bravo"}, new int[] {2, 2});
    rowMeta.setValueMetaList(setList);
    assertTrue(setList.contains(rowMeta.searchValueMeta("alpha")));
    assertTrue(setList.contains(rowMeta.searchValueMeta("bravo")));

    // check that it is avalable by index:
    assertEquals(0, rowMeta.indexOfValue("alpha"));
    assertEquals(1, rowMeta.indexOfValue("bravo"));
  }

  @Test
  void testSetValueMetaListNullName() throws HopPluginException {
    List<IValueMeta> setList = this.generateVList(new String[] {"alpha", null}, new int[] {2, 2});
    rowMeta.setValueMetaList(setList);
    assertTrue(setList.contains(rowMeta.searchValueMeta("alpha")));
    assertFalse(setList.contains(rowMeta.searchValueMeta(null)));

    // check that it is avalable by index:
    assertEquals(0, rowMeta.indexOfValue("alpha"));
    assertEquals(-1, rowMeta.indexOfValue(null));
  }

  @Test
  void testDeSynchronizationModifyingOriginalList() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          // remember 0-based arrays
          int size = rowMeta.size();
          // should be added at the end
          rowMeta.getValueMetaList().add(charly);
          assertEquals(size, rowMeta.indexOfValue("charly"));
        });
  }

  @Test
  void testExists() {
    assertTrue(rowMeta.exists(string));
    assertTrue(rowMeta.exists(date));
    assertTrue(rowMeta.exists(integer));
  }

  @Test
  void testAddValueMetaValueMetaInterface() {
    rowMeta.addValueMeta(charly);
    assertTrue(rowMeta.getValueMetaList().contains(charly));
  }

  @Test
  void testAddValueMetaNullName() {
    IValueMeta vmi = new ValueMetaBase();
    rowMeta.addValueMeta(vmi);
    assertTrue(rowMeta.getValueMetaList().contains(vmi));
  }

  @Test
  void testAddValueMetaIntValueMetaInterface() {
    rowMeta.addValueMeta(1, charly);
    assertEquals(1, rowMeta.getValueMetaList().indexOf(charly));
  }

  @Test
  void testGetValueMeta() {
    // see before method insertion order.
    assertEquals(rowMeta.getValueMeta(1), integer);
  }

  @Test
  void testSetValueMeta() {
    rowMeta.setValueMeta(1, charly);
    assertEquals(1, rowMeta.getValueMetaList().indexOf(charly));
    assertEquals(3, rowMeta.size(), "There is still 3 elements:");
    assertEquals(-1, rowMeta.indexOfValue("integer"));
  }

  @Test
  void testSetValueMetaDup() {
    rowMeta.setValueMeta(1, dup);
    assertEquals(3, rowMeta.size(), "There is still 3 elements:");
    assertEquals(-1, rowMeta.indexOfValue("integer"));

    rowMeta.setValueMeta(1, dup);
    assertEquals(3, rowMeta.size(), "There is still 3 elements:");
    assertEquals(-1, rowMeta.indexOfValue("integer"));

    rowMeta.setValueMeta(2, dup);
    assertEquals(3, rowMeta.size(), "There is still 3 elements:");
    assertEquals(1, rowMeta.getValueMetaList().indexOf(dup), "Original is still the same (object)");
    assertEquals(1, rowMeta.indexOfValue("dup"), "Original is still the same (name)");
    assertEquals(2, rowMeta.indexOfValue("dup_1"), "Renaming happened");
  }

  @Test
  void testSetValueMetaNullName() {
    IValueMeta vmi = new ValueMetaBase();
    rowMeta.setValueMeta(1, vmi);
    assertEquals(1, rowMeta.getValueMetaList().indexOf(vmi));
    assertEquals(3, rowMeta.size(), "There is still 3 elements:");
  }

  @Test
  void testIndexOfValue() {
    List<IValueMeta> list = rowMeta.getValueMetaList();
    assertEquals(0, list.indexOf(string));
    assertEquals(1, list.indexOf(integer));
    assertEquals(2, list.indexOf(date));
  }

  @Test
  void testIndexOfNullValue() {
    assertEquals(-1, rowMeta.indexOfValue(null));
  }

  @Test
  void testSearchValueMeta() {
    IValueMeta vmi = rowMeta.searchValueMeta("integer");
    assertEquals(integer, vmi);
    vmi = rowMeta.searchValueMeta("string");
    assertEquals(string, vmi);
    vmi = rowMeta.searchValueMeta("date");
    assertEquals(date, vmi);
  }

  @Test
  void testAddRowMeta() throws HopPluginException {
    List<IValueMeta> list =
        this.generateVList(
            new String[] {"alfa", "bravo", "charly", "delta"}, new int[] {2, 2, 3, 4});
    RowMeta added = new RowMeta();
    added.setValueMetaList(list);
    rowMeta.addRowMeta(added);

    assertEquals(7, rowMeta.getValueMetaList().size());
    assertEquals(5, rowMeta.indexOfValue("charly"));
  }

  @Test
  void testMergeRowMeta() throws HopPluginException {
    List<IValueMeta> list =
        this.generateVList(new String[] {"phobos", "demos", "mars"}, new int[] {6, 6, 6});
    list.add(1, integer);
    RowMeta toMerge = new RowMeta();
    toMerge.setValueMetaList(list);

    rowMeta.mergeRowMeta(toMerge);
    assertEquals(7, rowMeta.size());

    list = rowMeta.getValueMetaList();
    assertTrue(list.contains(integer));
    IValueMeta found = null;
    for (IValueMeta vm : list) {
      if (vm.getName().equals("integer_1")) {
        found = vm;
        break;
      }
    }
    assertNotNull(found);
  }

  @Test
  void testRemoveValueMetaString() throws HopValueException {
    rowMeta.removeValueMeta("string");
    assertEquals(2, rowMeta.size());
    assertNotNull(rowMeta.searchValueMeta("integer"));
    assertEquals("integer", rowMeta.searchValueMeta("integer").getName());
    assertNull(rowMeta.searchValueMeta("string"));
  }

  @Test
  void testRemoveValueMetaInt() {
    rowMeta.removeValueMeta(1);
    assertEquals(2, rowMeta.size());
    assertNotNull(rowMeta.searchValueMeta("date"));
    assertNotNull(rowMeta.searchValueMeta("string"));
    assertNull(rowMeta.searchValueMeta("notExists"));
    assertEquals("date", rowMeta.searchValueMeta("date").getName());
    assertNull(rowMeta.searchValueMeta("integer"));
  }

  @Test
  void testLowerCaseNamesSearch() {
    assertNotNull(rowMeta.searchValueMeta("Integer"));
    assertNotNull(rowMeta.searchValueMeta("string".toUpperCase()));
  }

  @Test
  void testMultipleSameNameInserts() {
    for (int i = 0; i < 13; i++) {
      rowMeta.addValueMeta(integer);
    }
    String resultName = "integer_13";
    assertEquals(resultName, rowMeta.searchValueMeta(resultName).getName());
  }

  @Test
  void testExternalValueMetaModification() {
    IValueMeta vmi = rowMeta.searchValueMeta("string");
    vmi.setName("string2");
    assertNotNull(rowMeta.searchValueMeta(vmi.getName()));
  }

  @Test
  void testSwapNames() throws HopPluginException {
    IValueMeta string2 = ValueMetaFactory.createValueMeta("string2", IValueMeta.TYPE_STRING);
    rowMeta.addValueMeta(string2);
    assertSame(string, rowMeta.searchValueMeta("string"));
    assertSame(string2, rowMeta.searchValueMeta("string2"));
    string.setName("string2");
    string2.setName("string");
    assertSame(string2, rowMeta.searchValueMeta("string"));
    assertSame(string, rowMeta.searchValueMeta("string2"));
  }

  @Test
  void testCopyRowMetaCacheConstructor() {
    Map<String, Integer> mapping = new HashMap<>();
    mapping.put("a", 1);
    RowMeta.RowMetaCache rowMetaCache = new RowMeta.RowMetaCache(mapping);
    RowMeta.RowMetaCache rowMetaCache2 = new RowMeta.RowMetaCache(rowMetaCache);
    assertEquals(rowMetaCache.mapping, rowMetaCache2.mapping);
    rowMetaCache = new RowMeta.RowMetaCache(mapping);
    rowMetaCache2 = new RowMeta.RowMetaCache(rowMetaCache);
    assertEquals(rowMetaCache.mapping, rowMetaCache2.mapping);
  }

  @Test
  void testNeedRealClone() {
    RowMeta newRowMeta = new RowMeta();
    newRowMeta.addValueMeta(string);
    newRowMeta.addValueMeta(integer);
    newRowMeta.addValueMeta(date);
    newRowMeta.addValueMeta(charly);
    newRowMeta.addValueMeta(dup);
    newRowMeta.addValueMeta(bin);
    List<Integer> list = newRowMeta.getOrCreateValuesThatNeedRealClone(newRowMeta.valueMetaList);
    assertEquals(3, list.size()); // Should be charly, dup and bin
    assertTrue(list.contains(3)); // charly
    assertTrue(list.contains(4)); // dup
    assertTrue(list.contains(5)); // bin
    newRowMeta.addValueMeta(charly); // should have nulled the newRowMeta.needRealClone
    assertNull(newRowMeta.needRealClone); // null because of the new add
    list = newRowMeta.getOrCreateValuesThatNeedRealClone(newRowMeta.valueMetaList);
    assertNotNull(newRowMeta.needRealClone);
    assertEquals(4, list.size()); // Should still be charly, dup, bin, charly_1
    newRowMeta.addValueMeta(bin); // add new binary, should null out needRealClone again
    assertNull(newRowMeta.needRealClone); // null because of the new add
    list = newRowMeta.getOrCreateValuesThatNeedRealClone(newRowMeta.valueMetaList);
    assertNotNull(newRowMeta.needRealClone);
    assertEquals(5, list.size()); // Should be charly, dup and bin, charly_1, bin_1

    newRowMeta.addValueMeta(string); // add new string, should null out needRealClone again
    assertNull(newRowMeta.needRealClone); // null because of the new add
    list = newRowMeta.getOrCreateValuesThatNeedRealClone(newRowMeta.valueMetaList);
    assertNotNull(newRowMeta.needRealClone);
    assertEquals(
        5,
        list.size()); // Should still only be charly, dup and bin, charly_1, bin_1 - adding a string
    // doesn't change of result
  }

  // @Test
  void hasedRowMetaListFasterWhenSearchByName() throws HopPluginException {
    rowMeta.clear();

    IValueMeta searchFor = null;
    for (int i = 0; i < 100000; i++) {
      IValueMeta vm =
          ValueMetaFactory.createValueMeta(UUID.randomUUID().toString(), IValueMeta.TYPE_STRING);
      rowMeta.addValueMeta(vm);
      if (i == 50000) {
        searchFor = vm;
      }
    }
    List<IValueMeta> vmList = rowMeta.getValueMetaList();

    // now see how fast we are.
    long start, stop, time1, time2;
    start = System.nanoTime();
    vmList.indexOf(searchFor);
    stop = System.nanoTime();
    time1 = stop - start;

    start = System.nanoTime();
    IValueMeta found = rowMeta.searchValueMeta(searchFor.getName());
    stop = System.nanoTime();
    assertEquals(searchFor, found);
    time2 = stop - start;

    assertTrue(
        time1 > time2,
        "array search is slower then current implementation : "
            + "for array list: "
            + time1
            + ", for hashed rowMeta: "
            + time2);
  }

  // @Test
  void hashedRowMetaListNotMuchSlowerThenIndexedAccess() throws HopPluginException {
    rowMeta = new RowMeta();

    // create pre-existed rom meta list
    List<IValueMeta> pre = new ArrayList<>(100000);
    for (int i = 0; i < 100000; i++) {
      IValueMeta vm =
          ValueMetaFactory.createValueMeta(UUID.randomUUID().toString(), IValueMeta.TYPE_STRING);
      pre.add(vm);
    }

    // now see how fast we are.

    long start, stop, time1, time2;
    start = System.nanoTime();
    // this is when filling regular array like in prev implementation
    stop = System.nanoTime();
    time1 = stop - start;

    start = System.nanoTime();
    for (IValueMeta item : pre) {
      rowMeta.addValueMeta(item);
    }
    stop = System.nanoTime();
    time2 = stop - start;

    // ~6 time slower that for original implementation
    // let say finally it is not 10 times slower :(
    assertTrue(time1 * 10 > time2, "it is not 10 times slower than for original arrayList");
  }

  @Test
  void testMergeRowMetaWithOriginTransform() throws Exception {

    List<IValueMeta> list =
        this.generateVList(new String[] {"phobos", "demos", "mars"}, new int[] {6, 6, 6});
    list.add(1, integer);
    RowMeta toMerge = new RowMeta();
    toMerge.setValueMetaList(list);

    rowMeta.mergeRowMeta(toMerge, "newOriginTransform");
    assertEquals(7, rowMeta.size());

    list = rowMeta.getValueMetaList();
    assertTrue(list.contains(integer));
    IValueMeta found = null;
    IValueMeta other = null;
    for (IValueMeta vm : list) {
      if (vm.getName().equals("integer_1")) {
        found = vm;
        break;
      } else {
        other = vm;
      }
    }
    assertNotNull(found);
    assertEquals("newOriginTransform", found.getOrigin());
    assertNotNull(other);
    assertEquals("originTransform", other.getOrigin());
  }

  @Test
  void testGetFieldNames() {
    rowMeta.clear();
    fillRowMeta();
    String[] names = rowMeta.getFieldNames();
    assertEquals(10, names.length);
    assertEquals("sample", names[0]);
    for (int i = 1; i < names.length; i++) {
      assertEquals("", names[i]);
    }
  }

  @Test
  void testHashCode() {
    rowMeta.clear();
    byte[] byteArray = new byte[] {49, 50, 51};
    Object[] objArray = new Object[] {byteArray};
    try {
      assertEquals(78512, rowMeta.hashCode(objArray));
    } catch (HopValueException e) {
      e.printStackTrace();
    }
  }

  private void fillRowMeta() {
    rowMeta.addValueMeta(0, new ValueMetaString("sample"));
    for (int i = 1; i < 10; i++) {
      rowMeta.addValueMeta(i, new ValueMetaInteger(null));
    }
  }
}
