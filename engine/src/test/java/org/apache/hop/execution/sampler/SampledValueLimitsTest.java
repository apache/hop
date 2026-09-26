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

package org.apache.hop.execution.sampler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Field;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.plugins.dataprof.BasicDataProfilingDataSampler;
import org.apache.hop.execution.sampler.plugins.dataprof.BasicDataProfilingDataSamplerStore;
import org.apache.hop.execution.sampler.plugins.first.FirstRowsExecutionDataSampler;
import org.apache.hop.execution.sampler.plugins.first.FirstRowsExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SampledValueLimitsTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void unlimitedCopyLeavesEveryValueInANewRow() throws Exception {
    IRowMeta rowMeta = sampleRowMeta();
    byte[] binary = new byte[] {1, 2, 3, 4};
    JsonNode json = JsonUtil.jsonMapper().readTree("{\"ok\":true}");
    GenericRecord avro = avroRecord("hi");
    Object[] row = new Object[] {"text", json, binary, avro};

    Object[] copy = SampledValueLimits.unlimited().copyRow(rowMeta, row);

    assertNotSame(row, copy);
    assertEquals("text", copy[0]);
    assertEquals(json, copy[1]);
    assertArrayEquals(binary, (byte[]) copy[2]);
    assertNotSame(binary, copy[2]);
    assertEquals("hi", ((GenericRecord) copy[3]).get("a").toString());
    assertSame("text", row[0]);
    assertSame(json, row[1]);
    assertSame(binary, row[2]);
    assertSame(avro, row[3]);
  }

  @Test
  void zeroOnOneTypeReplacesOnlyThatType() throws Exception {
    IRowMeta rowMeta = sampleRowMeta();
    Object[] row =
        new Object[] {
          "text", JsonUtil.jsonMapper().readTree("{\"ok\":true}"), new byte[] {9}, avroRecord("hi")
        };

    Object[] copy = limits(null, null, "0", null).copyRow(rowMeta, row);

    assertEquals("text", copy[0]);
    assertEquals(row[1], copy[1]);
    assertNull(copy[2]);
    assertEquals("hi", ((GenericRecord) copy[3]).get("a").toString());
    assertArrayEquals(new byte[] {9}, (byte[]) row[2]);
  }

  @Test
  void positiveStringLimitKeepsShortValuesAndReplacesLongOnes() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("body"));
    String longText = "this is longer than five";
    Object[] shortRow = new Object[] {"short"};
    Object[] longRow = new Object[] {longText};
    SampledValueLimits valueLimits = limits("5", null, null, null);

    Object[] shortCopy = valueLimits.copyRow(rowMeta, shortRow);
    Object[] longCopy = valueLimits.copyRow(rowMeta, longRow);

    assertEquals("short", shortCopy[0]);
    assertEquals(SampledValueLimits.notStored(longText.length(), "characters"), longCopy[0]);
    assertSame(longText, longRow[0]);
    assertNotSame(longRow, longCopy);
  }

  @Test
  void binaryAndAvroOverTheLimitAreNullAndSchemaBytesCount() throws Exception {
    IRowMeta rowMeta = sampleRowMeta();
    byte[] binary = new byte[] {1, 2, 3, 4, 5};
    GenericRecord small = avroRecord("hi");
    int smallSize = ValueMetaAvroRecord.storedPayloadBytes(small);
    GenericRecord wideSchema = wideSchemaRecord();
    int wideSize = ValueMetaAvroRecord.storedPayloadBytes(wideSchema);
    assertTrue(wideSize > smallSize);

    Object[] overBinary =
        limits(null, null, "4", null)
            .copyRow(rowMeta, new Object[] {"text", json("{\"a\":1}"), binary, small});
    assertNull(overBinary[2]);
    assertNotNull(overBinary[3]);

    Object[] overSchema =
        limits(null, null, null, Integer.toString(smallSize))
            .copyRow(rowMeta, new Object[] {"text", json("{\"a\":1}"), new byte[] {1}, wideSchema});
    assertNull(overSchema[3]);

    Object[] underSchema =
        limits(null, null, "0", Integer.toString(smallSize))
            .copyRow(rowMeta, new Object[] {"text", json("{\"a\":1}"), new byte[] {1}, small});
    assertNull(underSchema[2]);
    assertEquals("hi", ((GenericRecord) underSchema[3]).get("a").toString());
    assertNotSame(small, underSchema[3]);
  }

  @Test
  void nonNumericLimitIsUnlimited() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("body"));
    String longText = "still stored";
    Object[] row = new Object[] {longText};

    Object[] copy = limits("many", null, null, null).copyRow(rowMeta, row);

    assertEquals(longText, copy[0]);
  }

  @Test
  void variableLimitIsResolvedOnce() throws Exception {
    ExecutionDataProfile profile = new ExecutionDataProfile("profile");
    profile.setStringValueLimit("${MAX}");
    Variables variables = new Variables();
    variables.setVariable("MAX", "0");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("body"));

    Object[] copy =
        SampledValueLimits.from(profile, variables).copyRow(rowMeta, new Object[] {"kept?"});

    assertEquals(SampledValueLimits.NOT_STORED, copy[0]);
  }

  @Test
  void profileCopyKeepsTheLimitFields() {
    ExecutionDataProfile profile = new ExecutionDataProfile("profile");
    profile.setStringValueLimit("10");
    profile.setJsonValueLimit("0");
    profile.setBinaryValueLimit("20");
    profile.setAvroValueLimit("30");

    ExecutionDataProfile copy = new ExecutionDataProfile(profile);

    assertEquals("10", copy.getStringValueLimit());
    assertEquals("0", copy.getJsonValueLimit());
    assertEquals("20", copy.getBinaryValueLimit());
    assertEquals("30", copy.getAvroValueLimit());
  }

  @Test
  void limitsRoundTripThroughMetadata() throws Exception {
    ExecutionDataProfile profile = new ExecutionDataProfile("limits");
    profile.setStringValueLimit("10");
    profile.setJsonValueLimit("0");
    profile.setBinaryValueLimit("20");
    profile.setAvroValueLimit("${SAMPLE_AVRO_LIMIT}");
    IHopMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(ExecutionDataProfile.class).save(profile);

    ExecutionDataProfile loaded = provider.getSerializer(ExecutionDataProfile.class).load("limits");

    assertEquals("10", loaded.getStringValueLimit());
    assertEquals("0", loaded.getJsonValueLimit());
    assertEquals("20", loaded.getBinaryValueLimit());
    assertEquals("${SAMPLE_AVRO_LIMIT}", loaded.getAvroValueLimit());
  }

  @Test
  void profileDeclaresTheLimitWidgets() throws Exception {
    assertNotNull(ExecutionDataProfile.class.getAnnotation(GuiPlugin.class));
    assertLimitWidget("stringValueLimit");
    assertLimitWidget("jsonValueLimit");
    assertLimitWidget("binaryValueLimit");
    assertLimitWidget("avroValueLimit");
  }

  private static void assertLimitWidget(String fieldName) throws Exception {
    Field field = ExecutionDataProfile.class.getDeclaredField(fieldName);
    GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
    assertNotNull(element, fieldName);
    assertEquals(fieldName, element.id());
    assertEquals(GuiElementType.TEXT, element.type());
    assertEquals(ExecutionDataProfile.GUI_PLUGIN_LIMITS_PARENT_ID, element.parentId());
    assertEquals(GuiWidgetGroupType.BOXES, element.groupType());
    assertEquals("i18n::ExecutionDataProfile.Group.LargeValues", element.group());
    assertTrue(element.label().startsWith("i18n::"));
    assertTrue(element.toolTip().startsWith("i18n::"));
  }

  @Test
  void jacksonDoesNotWriteTheRuntimeLimits() throws Exception {
    FirstRowsExecutionDataSampler sampler = new FirstRowsExecutionDataSampler("10");
    sampler.setSampledValueLimits(limits("0", "0", "0", "0"));

    String json = HopJson.newMapper().writeValueAsString(sampler);

    assertFalse(json.contains("sampledValueLimits"));
    assertFalse(json.contains("stringLimit"));
  }

  @Test
  void profilingKeepsLengthStatsWithoutTheLongString() throws Exception {
    BasicDataProfilingDataSampler sampler = new BasicDataProfilingDataSampler();
    sampler.setSampledValueLimits(limits("5", null, null, null));
    ExecutionDataSamplerMeta samplerMeta =
        new ExecutionDataSamplerMeta("transform", "0", "log", false, true);
    BasicDataProfilingDataSamplerStore store = sampler.createSamplerStore(samplerMeta);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("body"));
    store.init(new Variables(), rowMeta, rowMeta);

    String longText = "this is long";
    sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, new Object[] {"a", "short"});
    sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, new Object[] {"b", longText});

    assertEquals(2L, store.getNonNullCounters().get("body"));
    assertEquals(longText.length(), store.getMaxLengths().get("body"));
    assertEquals("short", store.getMaxValues().get("body"));
    assertFalse(store.getMaxValues().containsValue(longText));

    RowBuffer lengthRows =
        store
            .getProfileSamples()
            .get("body")
            .get(BasicDataProfilingDataSampler.ProfilingType.MaxLength);
    assertEquals(1, lengthRows.size());
    Object[] example = lengthRows.getBuffer().get(0);
    assertEquals("b", example[0]);
    assertEquals(SampledValueLimits.notStored(longText.length(), "characters"), example[1]);
  }

  @Test
  void firstRowsReplaceJsonAndKeepTheOtherColumns() throws Exception {
    FirstRowsExecutionDataSampler sampler = new FirstRowsExecutionDataSampler("10");
    sampler.setSampledValueLimits(limits(null, "0", null, null));
    ExecutionDataSamplerMeta samplerMeta =
        new ExecutionDataSamplerMeta("transform", "0", "log", false, true);
    FirstRowsExecutionDataSamplerStore store = sampler.createSamplerStore(samplerMeta);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaJson("payload"));
    store.init(new Variables(), rowMeta, rowMeta);
    JsonNode payload = json("{\"n\":1}");
    Object[] row = new Object[] {"row-1", payload};

    sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, row);

    Object[] stored = store.getRows().get(0);
    assertNotSame(row, stored);
    assertEquals("row-1", stored[0]);
    JsonNode marker = assertInstanceOf(JsonNode.class, stored[1]);
    assertEquals(SampledValueLimits.NOT_STORED, marker.asText());
    assertSame(payload, row[1]);
  }

  private static SampledValueLimits limits(
      String stringLimit, String jsonLimit, String binaryLimit, String avroLimit) {
    ExecutionDataProfile profile = new ExecutionDataProfile("profile");
    profile.setStringValueLimit(stringLimit);
    profile.setJsonValueLimit(jsonLimit);
    profile.setBinaryValueLimit(binaryLimit);
    profile.setAvroValueLimit(avroLimit);
    return SampledValueLimits.from(profile, new Variables());
  }

  private static IRowMeta sampleRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    rowMeta.addValueMeta(new ValueMetaJson("json"));
    rowMeta.addValueMeta(new ValueMetaBinary("binary"));
    rowMeta.addValueMeta(new ValueMetaAvroRecord("avro"));
    return rowMeta;
  }

  private static JsonNode json(String text) throws Exception {
    return JsonUtil.jsonMapper().readTree(text);
  }

  private static GenericRecord avroRecord(String value) {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("a", value);
    return record;
  }

  private static GenericRecord wideSchemaRecord() {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Wide\",\"doc\":\""
                    + "x".repeat(400)
                    + "\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("a", "hi");
    return record;
  }
}
