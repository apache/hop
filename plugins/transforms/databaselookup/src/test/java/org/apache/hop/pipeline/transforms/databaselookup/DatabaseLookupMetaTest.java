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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabaseLookupMetaTest {

  private DatabaseLookupMeta databaseLookupMeta;
  private IHopMetadataProvider metadataProvider;

  @BeforeEach
  void before() throws Exception {
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
  void getFieldWithValueUsedTwice() throws HopTransformException {

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
              new ValueMetaString("value"),
              new ValueMetaString("v1"),
              new ValueMetaString("v2"),
              new ValueMetaString("v3"),
            });
    assertEquals(4, row.getValueMetaList().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(expectedRow.get(i).getName(), row.getValueMetaList().get(i).getName());
    }
  }

  @Test
  void getFieldsInfersTypeFromInfoWhenDefaultTypeEmpty() throws Exception {
    Lookup lookup = databaseLookupMeta.getLookup();
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "stock_name",
                "",
                "",
                "",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    IRowMeta[] info = new IRowMeta[1];
    info[0] = new RowMeta();
    info[0].addValueMeta(new ValueMetaString("stock_name"));

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("id"));

    databaseLookupMeta.getFields(row, "Database lookup", info, null, null, null);

    assertEquals(2, row.size());
    IValueMeta stockName = row.searchValueMeta("stock_name");
    assertNotNull(stockName);
    assertEquals(IValueMeta.TYPE_STRING, stockName.getType());
  }

  @Test
  void getFieldsInfersTypeFromInfoWhenDefaultTypeNone() throws Exception {
    Lookup lookup = databaseLookupMeta.getLookup();
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "amount",
                "amt",
                "",
                "None",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    IRowMeta[] info = new IRowMeta[1];
    info[0] = new RowMeta();
    info[0].addValueMeta(new ValueMetaInteger("amount"));

    IRowMeta row = new RowMeta();
    databaseLookupMeta.getFields(row, "Database lookup", info, null, null, null);

    assertEquals(1, row.size());
    IValueMeta amt = row.searchValueMeta("amt");
    assertNotNull(amt);
    assertEquals(IValueMeta.TYPE_INTEGER, amt.getType());
  }

  @Test
  void getFieldsExplicitTypeOverridesInfoType() throws Exception {
    Lookup lookup = databaseLookupMeta.getLookup();
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "amount",
                "",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    IRowMeta[] info = new IRowMeta[1];
    info[0] = new RowMeta();
    info[0].addValueMeta(new ValueMetaInteger("amount"));

    IRowMeta row = new RowMeta();
    databaseLookupMeta.getFields(row, "Database lookup", info, null, null, null);

    assertEquals(1, row.size());
    IValueMeta amount = row.searchValueMeta("amount");
    assertNotNull(amount);
    assertEquals(IValueMeta.TYPE_STRING, amount.getType());
  }

  @Test
  void getFieldsThrowsWhenTypeMissingAndFieldNotInInfo() {
    Lookup lookup = databaseLookupMeta.getLookup();
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "missing_col",
                "",
                "",
                "",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    IRowMeta[] info = new IRowMeta[1];
    info[0] = new RowMeta();
    info[0].addValueMeta(new ValueMetaString("other"));

    IRowMeta row = new RowMeta();
    HopTransformException thrown =
        assertThrows(
            HopTransformException.class,
            () -> databaseLookupMeta.getFields(row, "Database lookup", info, null, null, null));
    assertTrue(thrown.getMessage().contains("missing_col"));
  }

  @Test
  void cloneTest() throws Exception {
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
    assertNotSame(meta2, meta);
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
  void testXmlRoundTrip() throws Exception {
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
  void testInjection() throws Exception {
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
    assertNotNull(meta.getConnection());
    assertEquals("postgres", meta.getConnection());

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

  @Test
  void supportsErrorHandling_ReturnsTrue() {
    assertTrue(databaseLookupMeta.supportsErrorHandling());
  }

  @Test
  void getConditionStrings_ContainsAllOperators() {
    List<String> conditions = DatabaseLookupMeta.getConditionStrings();
    assertEquals(10, conditions.size());
    assertTrue(conditions.contains("="));
    assertTrue(conditions.contains("<>"));
    assertTrue(conditions.contains("LIKE"));
    assertTrue(conditions.contains("BETWEEN"));
    assertTrue(conditions.contains("IS NULL"));
    assertTrue(conditions.contains("IS NOT NULL"));
  }

  @Test
  void check_ReportsErrorWhenConnectionMissing() {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setConnection("");
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta("lookup", meta);
    Variables variables = new Variables();

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        new RowMeta(),
        new String[] {"prev"},
        new String[] {},
        null,
        variables,
        metadataProvider);

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "Expected a missing-connection error remark");
  }

  @Test
  void check_ReportsErrorWhenNoInputReceived() {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setConnection("postgres");
    meta.getLookup().setTableName("");
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta("lookup", meta);
    Variables variables = new Variables();

    // Connection exists in metadata but connecting to NONE DB may fail — still should report
    // no-input when input array is empty.
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        new RowMeta(),
        new String[] {},
        new String[] {},
        null,
        variables,
        metadataProvider);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText() != null
                        && r.getText().toLowerCase().contains("no input")),
        "Expected a no-input error remark, got: " + remarks);
  }

  @Test
  void check_ReportsOkWhenReceivingInput() {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setConnection("postgres");
    meta.getLookup().setTableName("");
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta("lookup", meta);
    Variables variables = new Variables();

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        transformMeta,
        new RowMeta(),
        new String[] {"prev"},
        new String[] {},
        null,
        variables,
        metadataProvider);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_OK
                        && r.getText() != null
                        && r.getText().toLowerCase().contains("receiving")),
        "Expected an OK remark about receiving input, got: " + remarks);
  }

  @Test
  void analyseImpact_AddsReadImpactForKeysAndReturns() throws Exception {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setConnection("postgres");
    Lookup lookup = meta.getLookup();
    lookup.setTableName("users");
    lookup.getKeyFields().add(new KeyField("in_id", "", "=", "id"));
    lookup
        .getReturnValues()
        .add(
            new ReturnValue(
                "name",
                "user_name",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("in_id"));

    List<DatabaseImpact> impact = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta("lookup", meta);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(pipelineMeta.getName()).thenReturn("pipe");

    meta.analyseImpact(
        new Variables(),
        impact,
        pipelineMeta,
        transformMeta,
        prev,
        new String[] {},
        new String[] {},
        null,
        metadataProvider);

    assertEquals(2, impact.size());
    assertEquals(DatabaseImpact.TYPE_IMPACT_READ, impact.get(0).getType());
    assertEquals("id", impact.get(0).getField());
    assertEquals(DatabaseImpact.TYPE_IMPACT_READ, impact.get(1).getType());
    assertEquals("name", impact.get(1).getField());
  }
}
