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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class DatabaseLookupMetaTest {

  private DatabaseLookupMeta databaseLookupMeta;
  private IHopMetadataProvider metadataProvider;

  @Before
  public void before() throws Exception {
    HopClientEnvironment.init();
    databaseLookupMeta = new DatabaseLookupMeta();
    metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(DatabaseMeta.class)
        .save(
            new DatabaseMeta(
                "postgres", "NONE", "JDBC", "localhost", "test", "${PORT}", "hop", "pass"));
  }

  @Test
  public void getFieldWithValueUsedTwice() throws HopTransformException {

    Lookup lookup = databaseLookupMeta.getLookup();
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "match",
                "v1",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "match",
                "v2",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "mismatch",
                "v3",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    IValueMeta v1 = new ValueMetaString("match");
    IValueMeta v2 = new ValueMetaString("match1");
    IRowMeta[] info = new IRowMeta[1];
    info[0] = new RowMeta();
    info[0].setValueMetaList(Arrays.asList(v1, v2));

    IValueMeta r1 = new ValueMetaString("value");
    IRowMeta row = new RowMeta();
    row.setValueMetaList(new ArrayList<>(Arrays.asList(r1)));

    databaseLookupMeta.getFields(row, "", info, null, null, null);

    List<IValueMeta> expectedRow =
        Arrays.asList(
            new IValueMeta[] {
              new ValueMetaString("value"), new ValueMetaString("v1"), new ValueMetaString("v2"),
            });
    assertEquals(3, row.getValueMetaList().size());
    for (int i = 0; i < 3; i++) {
      assertEquals(expectedRow.get(i).getName(), row.getValueMetaList().get(i).getName());
    }
  }

  @Test
  public void testProvidesModelerMeta() throws Exception {

    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    Lookup lookup = meta.getLookup();
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "f1",
                "s4",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "f2",
                "s5",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "f3",
                "s6",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    DatabaseLookupData databaseLookupData = new DatabaseLookupData();

    databaseLookupData.returnMeta = Mockito.mock(RowMeta.class);
    assertEquals(
        databaseLookupData.returnMeta, meta.getRowMeta(new Variables(), databaseLookupData));
    assertEquals(3, meta.getDatabaseFields().size());
    assertEquals("f1", meta.getDatabaseFields().get(0));
    assertEquals("f2", meta.getDatabaseFields().get(1));
    assertEquals("f3", meta.getDatabaseFields().get(2));
    assertEquals(3, meta.getStreamFields().size());
    assertEquals("s4", meta.getStreamFields().get(0));
    assertEquals("s5", meta.getStreamFields().get(1));
    assertEquals("s6", meta.getStreamFields().get(2));
  }

  @Test
  public void cloneTest() throws Exception {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setCached(true);
    meta.setCacheSize(123456);
    meta.setLoadingAllDataInCache(true);
    Lookup lookup = meta.getLookup();
    lookup.getKeyFields().add(new KeyField("aa", "gg", "ee", "cc"));
    lookup.getKeyFields().add(new KeyField("bb", "hh", "ff", "dd"));
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "ii",
                "kk",
                "mm",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_BOTH)));
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "jj",
                "ll",
                "nn",
                "Integer",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));
    lookup.setOrderByClause("FOO DESC");
    lookup.setEatingRowOnLookupFailure(true);
    lookup.setFailingOnMultipleResults(true);
    lookup.setSchemaName("SCHEMA");
    lookup.setTableName("TABLE");
    DatabaseLookupMeta meta2 = meta.clone();
    assertFalse(meta2 == meta);
    Lookup lookup2 = meta2.getLookup();

    assertEquals(meta.getSchemaName(), meta2.getSchemaName());
    assertEquals(meta.getTableName(), meta2.getTableName());
    assertEquals(meta.isCached(), meta2.isCached());
    assertEquals(meta.getCacheSize(), meta2.getCacheSize());
    assertEquals(meta.isLoadingAllDataInCache(), meta2.isLoadingAllDataInCache());

    assertEquals(lookup.getKeyFields().size(), lookup2.getKeyFields().size());
    for (int i = 0; i < lookup.getKeyFields().size(); i++) {
      KeyField k1 = lookup.getKeyFields().get(i);
      KeyField k2 = lookup2.getKeyFields().get(i);
      assertEquals(k1.getTableField(), k2.getTableField());
      assertEquals(k1.getCondition(), k2.getCondition());
      assertEquals(k1.getStreamField1(), k2.getStreamField1());
      assertEquals(k1.getStreamField2(), k2.getStreamField2());
    }
    assertEquals(lookup.getReturnValues().size(), lookup2.getReturnValues().size());
    for (int i = 0; i < lookup.getReturnValues().size(); i++) {
      ReturnValue r1 = lookup.getReturnValues().get(i);
      ReturnValue r2 = lookup2.getReturnValues().get(i);
      assertEquals(r1.getTableField(), r2.getTableField());
      assertEquals(r1.getNewName(), r2.getNewName());
      assertEquals(r1.getDefaultValue(), r2.getDefaultValue());
      assertEquals(r1.getDefaultType(), r2.getDefaultType());
      assertEquals(r1.getTrimType(), r2.getTrimType());
    }

    assertEquals(lookup.isEatingRowOnLookupFailure(), lookup2.isEatingRowOnLookupFailure());
    assertEquals(lookup.isFailingOnMultipleResults(), lookup2.isFailingOnMultipleResults());
    assertEquals(lookup.getOrderByClause(), lookup2.getOrderByClause());

    assertEquals(meta.getXml(), meta2.getXml());
  }

  @Test
  public void testXmlRoundTrip() throws Exception {
    String tag = TransformMeta.XML_TAG;

    Path path = Paths.get(getClass().getResource("/transform1.snippet").toURI());
    String xml = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    String transformXml = XmlHandler.openTag(tag) + xml + XmlHandler.closeTag(tag);
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml, tag),
        DatabaseLookupMeta.class,
        meta,
        metadataProvider);

    Lookup lookup = meta.getLookup();

    assertNotNull(lookup);
    assertEquals(1, lookup.getKeyFields().size());
    assertEquals(2, lookup.getReturnValues().size());
    String xml2 = meta.getXml();

    DatabaseLookupMeta meta2 = new DatabaseLookupMeta();
    String transformXml2 = XmlHandler.openTag(tag) + xml2 + XmlHandler.closeTag(tag);

    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml2, TransformMeta.XML_TAG),
        DatabaseLookupMeta.class,
        meta2,
        metadataProvider);

    // Compare meta1 and meta2 to see if all serialization survived correctly...
    //
    assertEquals(meta.getCacheSize(), meta2.getCacheSize());
    assertEquals(meta.isCached(), meta2.isCached());
    assertEquals(meta.isLoadingAllDataInCache(), meta2.isLoadingAllDataInCache());

    Lookup lookup2 = meta2.getLookup();
    assertEquals(lookup.getTableName(), lookup2.getTableName());
    assertEquals(lookup.getSchemaName(), lookup2.getSchemaName());
    assertEquals(lookup.getOrderByClause(), lookup2.getOrderByClause());

    assertEquals(1, lookup2.getKeyFields().size());
    assertEquals(2, lookup2.getReturnValues().size());
    for (int i = 0; i < lookup.getKeyFields().size(); i++) {
      KeyField key1 = lookup.getKeyFields().get(i);
      KeyField key2 = lookup2.getKeyFields().get(i);

      assertEquals(key1.getTableField(), key2.getTableField());
      assertEquals(key1.getStreamField1(), key2.getStreamField1());
      assertEquals(key1.getStreamField2(), key2.getStreamField2());
      assertEquals(key1.getCondition(), key2.getCondition());
    }
    for (int i = 0; i < lookup.getReturnValues().size(); i++) {
      ReturnValue returnValue1 = lookup.getReturnValues().get(i);
      ReturnValue returnValue2 = lookup2.getReturnValues().get(i);

      assertEquals(returnValue1.getTableField(), returnValue2.getTableField());
      assertEquals(returnValue1.getNewName(), returnValue2.getNewName());
      assertEquals(returnValue1.getDefaultValue(), returnValue2.getDefaultValue());
      assertEquals(returnValue1.getDefaultType(), returnValue2.getDefaultType());
    }
  }

  @Test
  public void testInjection() throws Exception {
    BeanInjectionInfo<DatabaseLookupMeta> injectionInfo =
        new BeanInjectionInfo<>(DatabaseLookupMeta.class);
    BeanInjector<DatabaseLookupMeta> injector = new BeanInjector<>(injectionInfo, metadataProvider);

    DatabaseLookupMeta meta = new DatabaseLookupMeta();

    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addString("database")
            .addString("schema")
            .addString("table")
            .addString("cache?")
            .addString("cacheSize")
            .addString("loadAll?")
            .addString("orderBy")
            .addString("failOnMultiple?")
            .addString("eatRow?")
            .build();
    List<RowMetaAndData> rows =
        Arrays.asList(
            new RowMetaAndData(
                rowMeta,
                "postgres",
                "schema1",
                "table1",
                "Y",
                "123",
                "Y",
                "field1 DESC",
                "Y",
                "Y"));

    injector.setProperty(meta, "connection", rows, "database");
    assertNotNull(meta.getDatabaseMeta());
    assertEquals("postgres", meta.getDatabaseMeta().getName());

    injector.setProperty(meta, "cache", rows, "cache?");
    assertTrue(meta.isCached());
    injector.setProperty(meta, "cache_size", rows, "cacheSize");
    assertEquals(123, meta.getCacheSize());
    injector.setProperty(meta, "cache_load_all", rows, "loadAll?");
    assertTrue(meta.isLoadingAllDataInCache());
    injector.setProperty(meta, "schema", rows, "schema");
    assertEquals("schema1", meta.getLookup().getSchemaName());
    injector.setProperty(meta, "table", rows, "table");
    assertEquals("table1", meta.getLookup().getTableName());
    injector.setProperty(meta, "orderby", rows, "orderBy");
    assertEquals("field1 DESC", meta.getLookup().getOrderByClause());
    injector.setProperty(meta, "fail_on_multiple", rows, "failOnMultiple?");
    assertTrue(meta.getLookup().isFailingOnMultipleResults());
    injector.setProperty(meta, "eat_row_on_failure", rows, "eatRow?");
    assertTrue(meta.getLookup().isEatingRowOnLookupFailure());

    // Keys...
    //
    IRowMeta keyRowMeta =
        new RowMetaBuilder()
            .addString("tableField")
            .addString("condition")
            .addString("stream1")
            .addString("stream2")
            .build();
    List<RowMetaAndData> keyRows =
        Arrays.asList(
            new RowMetaAndData(keyRowMeta, "tableField1", "=", "inputField11", "inputField12"),
            new RowMetaAndData(keyRowMeta, "tableField2", "=", "inputField21", "inputField22"));

    injector.setProperty(meta, "key_input_field1", keyRows, "stream1");
    injector.setProperty(meta, "key_input_field2", keyRows, "stream2");
    injector.setProperty(meta, "key_condition", keyRows, "condition");
    injector.setProperty(meta, "key_table_field", keyRows, "tableField");

    assertEquals(2, meta.getLookup().getKeyFields().size());
    assertEquals("tableField1", meta.getLookup().getKeyFields().get(0).getTableField());
    assertEquals("=", meta.getLookup().getKeyFields().get(0).getCondition());
    assertEquals("inputField11", meta.getLookup().getKeyFields().get(0).getStreamField1());
    assertEquals("inputField12", meta.getLookup().getKeyFields().get(0).getStreamField2());
    assertEquals("tableField2", meta.getLookup().getKeyFields().get(1).getTableField());
    assertEquals("=", meta.getLookup().getKeyFields().get(1).getCondition());
    assertEquals("inputField21", meta.getLookup().getKeyFields().get(1).getStreamField1());
    assertEquals("inputField22", meta.getLookup().getKeyFields().get(1).getStreamField2());

    // Return values...
    IRowMeta returnRowMeta =
        new RowMetaBuilder()
            .addString("tableField")
            .addString("rename")
            .addString("default")
            .addString("type")
            .build();
    List<RowMetaAndData> returnRows =
        Arrays.asList(new RowMetaAndData(returnRowMeta, "tableField3", "f3", "?", "String"));

    injector.setProperty(meta, "return_table_field", returnRows, "tableField");
    injector.setProperty(meta, "return_rename", returnRows, "rename");
    injector.setProperty(meta, "return_default_value", returnRows, "default");
    injector.setProperty(meta, "return_default_type", returnRows, "type");

    assertEquals(1, meta.getLookup().getReturnValues().size());
    assertEquals("tableField3", meta.getLookup().getReturnValues().get(0).getTableField());
    assertEquals("f3", meta.getLookup().getReturnValues().get(0).getNewName());
    assertEquals("?", meta.getLookup().getReturnValues().get(0).getDefaultValue());
    assertEquals("String", meta.getLookup().getReturnValues().get(0).getDefaultType());
  }
}
