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
 *
 */

package org.apache.hop.metadata.inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.inject.company.Company;
import org.apache.hop.metadata.inject.company.Employee;
import org.apache.hop.metadata.inject.sample.Field;
import org.apache.hop.metadata.inject.sample.SampleMeta;
import org.apache.hop.metadata.inject.sample.SampleType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HopMetadataInjectorTest {
  @BeforeAll
  static void beforeAll() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(),
      ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(),
      ValueMetaNumber.class.getName(),
      ValueMetaJson.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
    registry.registerPluginClass(
        NoneDatabaseMeta.class.getName(), DatabasePluginType.class, DatabaseMetaPlugin.class);
  }

  @Test
  void testCompanyInjection() throws Exception {
    Map<String, Object> injectionKeyMap = new HashMap<>();
    Map<String, RowBuffer> injectionGroupMap = new HashMap<>();

    Company company = new Company();

    // We inject the name and 2 employees
    injectionKeyMap.put("NAME", "ACME");

    RowBuffer rowBuffer = new RowBuffer();
    rowBuffer.setRowMeta(
        new RowMetaBuilder().addString("FIRST_NAME").addString("LAST_NAME").build());
    rowBuffer.addRow("Donald", "Duck");
    rowBuffer.addRow("Micky", "Mouse");
    injectionGroupMap.put("EMPLOYEES", rowBuffer);

    HopMetadataInjector.inject(
        new MemoryMetadataProvider(), company, injectionKeyMap, injectionGroupMap);

    assertEquals("ACME", company.getName());
    assertEquals(2, company.getEmployees().size());
    Employee e = company.getEmployees().get(0);
    assertEquals("Donald", e.getFirstName());
    assertEquals("Duck", e.getLastName());
    e = company.getEmployees().get(1);
    assertEquals("Micky", e.getFirstName());
    assertEquals("Mouse", e.getLastName());
  }

  @Test
  void testCompanyMapping() throws Exception {
    Map<String, Set<String>> map = HopMetadataInjector.findInjectionGroupKeys(Company.class);
    assertNotNull(map);
    assertEquals(1, map.size());
    Set<String> keys = map.get("EMPLOYEES");
    assertEquals(2, keys.size());
    assertTrue(keys.contains("FIRST_NAME"));
    assertTrue(keys.contains("LAST_NAME"));
  }

  @Test
  void testSampleMetaInjection() throws Exception {
    Map<String, Object> injectionKeyMap = new HashMap<>();
    Map<String, RowBuffer> injectionGroupMap = new HashMap<>();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider
        .getSerializer(DatabaseMeta.class)
        .save(new DatabaseMeta("EDW", "NONE", "", "", "", "", "", ""));

    SampleMeta sampleMeta = new SampleMeta();
    injectionKeyMap.put("FILE_NAME", "filename.txt");
    injectionKeyMap.put("LIMIT", "123");
    injectionKeyMap.put("IGNORE_ERRORS", "Y");
    injectionKeyMap.put("MAX_ERRORS", 999);
    injectionKeyMap.put("SAMPLE_TYPE", "-2-");
    injectionKeyMap.put("CONNECTION_NAME", "EDW");

    RowBuffer fieldsBuffer = new RowBuffer();
    fieldsBuffer.setRowMeta(
        new RowMetaBuilder()
            .addString("FIELD_NAME")
            .addString("FIELD_TYPE")
            .addInteger("FIELD_LENGTH")
            .addInteger("FIELD_PRECISION")
            .addString("FIELD_TRIM_TYPE")
            .build());
    fieldsBuffer.addRow("f1", "String", 100, -1, "right");
    fieldsBuffer.addRow("f2", "Integer", 7, 0, "both");
    fieldsBuffer.addRow("f3", "Number", 9, 2, "left");
    injectionGroupMap.put("FIELDS", fieldsBuffer);
    injectionGroupMap.put("MORE_FIELDS", fieldsBuffer);

    RowBuffer stringsBuffer = new RowBuffer();
    stringsBuffer.setRowMeta(new RowMetaBuilder().addString("STRING").build());
    stringsBuffer.addRow("string1");
    stringsBuffer.addRow("string2");
    injectionGroupMap.put("STRINGS", stringsBuffer);

    HopMetadataInjector.inject(provider, sampleMeta, injectionKeyMap, injectionGroupMap);

    assertEquals("filename.txt", sampleMeta.getFileName());
    assertEquals(123, sampleMeta.getLimit());
    assertTrue(sampleMeta.getAdditional().isIgnoreErrors());
    assertEquals(999, sampleMeta.getAdditional().getMaxErrors());
    assertEquals(SampleType.TWO, sampleMeta.getSampleType());
    assertEquals("EDW", sampleMeta.getDatabaseMeta().getName());

    assertEquals(2, sampleMeta.getStrings().size());
    assertEquals("string1", sampleMeta.getStrings().get(0));
    assertEquals("string2", sampleMeta.getStrings().get(1));

    assertEquals(3, sampleMeta.getFields().size());
    validateSampleMetaFields(sampleMeta.getFields());

    assertEquals(3, sampleMeta.getMoreFields().size());
    validateSampleMetaFields(sampleMeta.getMoreFields());
  }

  private static void validateSampleMetaFields(List<Field> fields) {
    Field f = fields.get(0);
    assertEquals("f1", f.getName());
    assertEquals(IValueMeta.TYPE_STRING, f.getHopType());
    assertEquals(100, f.getLength());
    assertEquals(-1, f.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_RIGHT, f.getTrimType());

    f = fields.get(1);
    assertEquals("f2", f.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, f.getHopType());
    assertEquals(7, f.getLength());
    assertEquals(0, f.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, f.getTrimType());

    f = fields.get(2);
    assertEquals("f3", f.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, f.getHopType());
    assertEquals(9, f.getLength());
    assertEquals(2, f.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_LEFT, f.getTrimType());
  }

  @Test
  void testSampleMetaMapping() throws Exception {
    Map<String, Set<String>> map = HopMetadataInjector.findInjectionGroupKeys(SampleMeta.class);
    assertNotNull(map);
    assertEquals(3, map.size());
    Set<String> strings = map.get("STRINGS");
    assertEquals(1, strings.size());
    assertTrue(strings.contains("STRING"));

    Set<String> fields = map.get("FIELDS");
    assertEquals(5, fields.size());
    assertTrue(fields.contains("FIELD_NAME"));
    assertTrue(fields.contains("FIELD_TYPE"));
    assertTrue(fields.contains("FIELD_LENGTH"));
    assertTrue(fields.contains("FIELD_PRECISION"));
    assertTrue(fields.contains("FIELD_TRIM_TYPE"));

    Set<String> moreFields = map.get("MORE_FIELDS");
    assertEquals(5, moreFields.size());
    assertTrue(moreFields.contains("FIELD_NAME"));
    assertTrue(moreFields.contains("FIELD_TYPE"));
    assertTrue(moreFields.contains("FIELD_LENGTH"));
    assertTrue(moreFields.contains("FIELD_PRECISION"));
    assertTrue(moreFields.contains("FIELD_TRIM_TYPE"));
  }
}
