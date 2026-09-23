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

package org.apache.hop.parquet.transforms.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link ParquetInputMeta} */
class ParquetInputMetaTest {

  @TempDir private Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  /** One column per Parquet type the schema extraction has a rule for. */
  private static MessageType everyTypeSchema() {
    return Types.buildMessage()
        .optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("s")
        .optional(PrimitiveTypeName.INT64)
        .named("i64")
        .optional(PrimitiveTypeName.INT32)
        .named("i32")
        .optional(PrimitiveTypeName.INT32)
        .as(LogicalTypeAnnotation.intType(8, true))
        .named("i8")
        .optional(PrimitiveTypeName.DOUBLE)
        .named("d")
        .optional(PrimitiveTypeName.FLOAT)
        .named("f")
        .optional(PrimitiveTypeName.BOOLEAN)
        .named("b")
        .optional(PrimitiveTypeName.BINARY)
        .named("bin")
        .optional(PrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))
        .named("ts")
        .optional(PrimitiveTypeName.INT32)
        .as(LogicalTypeAnnotation.timeType(true, TimeUnit.MILLIS))
        .named("t")
        .optional(PrimitiveTypeName.INT32)
        .as(LogicalTypeAnnotation.dateType())
        .named("date")
        .optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.jsonType())
        .named("json")
        .optional(PrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.decimalType(2, 10))
        .named("dec")
        .optional(PrimitiveTypeName.INT96)
        .named("i96")
        .optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.enumType())
        .named("other")
        .named("everything");
  }

  @Test
  void extractRowMetaMapsEveryParquetTypeToAHopType() throws Exception {
    String filename = tempDir.resolve("types.parquet").toString();
    ParquetTestFiles.write(
        filename,
        everyTypeSchema(),
        g -> g.append("s", "x").append("i64", 1L).append("i32", 2).append("b", true));

    IRowMeta rowMeta = ParquetInputMeta.extractRowMeta(new Variables(), filename);

    assertEquals(15, rowMeta.size());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.searchValueMeta("s").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.searchValueMeta("i64").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.searchValueMeta("i32").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.searchValueMeta("i8").getType());
    assertEquals(IValueMeta.TYPE_NUMBER, rowMeta.searchValueMeta("d").getType());
    assertEquals(IValueMeta.TYPE_NUMBER, rowMeta.searchValueMeta("f").getType());
    assertEquals(IValueMeta.TYPE_BOOLEAN, rowMeta.searchValueMeta("b").getType());
    assertEquals(IValueMeta.TYPE_BINARY, rowMeta.searchValueMeta("bin").getType());
    assertEquals(IValueMeta.TYPE_TIMESTAMP, rowMeta.searchValueMeta("ts").getType());
    assertEquals(IValueMeta.TYPE_TIMESTAMP, rowMeta.searchValueMeta("t").getType());
    assertEquals(IValueMeta.TYPE_DATE, rowMeta.searchValueMeta("date").getType());
    assertEquals(IValueMeta.TYPE_JSON, rowMeta.searchValueMeta("json").getType());
    assertEquals(IValueMeta.TYPE_BIGNUMBER, rowMeta.searchValueMeta("dec").getType());
    assertEquals(IValueMeta.TYPE_TIMESTAMP, rowMeta.searchValueMeta("i96").getType());
    // An annotation without a rule falls back to String.
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.searchValueMeta("other").getType());
  }

  @Test
  void extractRowMetaWrapsReadFailures() {
    Path missing = tempDir.resolve("missing.parquet");
    HopException e =
        assertThrows(
            HopException.class,
            () -> ParquetInputMeta.extractRowMeta(new Variables(), missing.toString()));
    assertTrue(e.getMessage().contains("missing.parquet"));
  }

  @Test
  void getFieldsAddsTheConfiguredFields() throws Exception {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.getFields().add(new ParquetField("a", "id", "Integer", null, "10", "0"));
    meta.getFields().add(new ParquetField("b", "amount", "Number", "#.00", "-1", "2"));

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("existing"));
    meta.getFields(rowMeta, "Parquet In", null, null, new Variables(), null);

    assertEquals(3, rowMeta.size());
    IValueMeta id = rowMeta.getValueMeta(1);
    assertEquals("id", id.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, id.getType());
    assertEquals(10, id.getLength());
    assertEquals("Parquet In", id.getOrigin());
    IValueMeta amount = rowMeta.getValueMeta(2);
    assertEquals(IValueMeta.TYPE_NUMBER, amount.getType());
    assertEquals("#.00", amount.getConversionMask());
    assertEquals(2, amount.getPrecision());
  }

  @Test
  void getFieldsFallsBackToTheMetadataFileWhenNoFieldsAreConfigured() throws Exception {
    String filename = tempDir.resolve("meta.parquet").toString();
    ParquetTestFiles.write(
        filename,
        Types.buildMessage()
            .required(PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("row"),
        g -> g.append("id", 1L).append("name", Binary.fromString("one")));

    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setMetadataFilename("${DIR}/meta.parquet");
    Variables variables = new Variables();
    variables.setVariable("DIR", tempDir.toString());

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "Parquet In", null, null, variables, null);

    assertEquals(2, rowMeta.size());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());
    assertEquals("name", rowMeta.getValueMeta(1).getName());
  }

  @Test
  void getFieldsReportsAnUnreadableMetadataFile() {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setMetadataFilename(tempDir.resolve("nope.parquet").toString());
    RowMeta rowMeta = new RowMeta();
    assertThrows(
        HopTransformException.class,
        () -> meta.getFields(rowMeta, "Parquet In", null, null, new Variables(), null));
  }

  @Test
  void serializesToXmlAndBack() throws Exception {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");
    meta.setMetadataFilename("/tmp/meta.parquet");
    meta.setSendingNullsRowWhenEmpty(true);
    meta.getFields().add(new ParquetField("a", "b", "String", "fmt", "5", "1"));

    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    ParquetInputMeta copy = new ParquetInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        ParquetInputMeta.class,
        copy,
        new MemoryMetadataProvider());

    assertEquals("filename", copy.getFilenameField());
    assertEquals("/tmp/meta.parquet", copy.getMetadataFilename());
    assertTrue(copy.isSendingNullsRowWhenEmpty());
    assertEquals(1, copy.getFields().size());
    ParquetField field = copy.getFields().get(0);
    assertEquals("a", field.getSourceField());
    assertEquals("b", field.getTargetField());
    assertEquals("String", field.getTargetType());
    assertEquals("fmt", field.getTargetFormat());
    assertEquals("5", field.getTargetLength());
    assertEquals("1", field.getTargetPrecision());

    // The copy constructor keeps the field list independent.
    ParquetField clone = new ParquetField(field);
    clone.setTargetField("c");
    assertEquals("b", field.getTargetField());
  }
}
