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

package org.apache.hop.avro.transforms.avrodecode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Avro decode over several records, one of them with a different type for the same field. The
 * transform runs in the test thread, so the value metadata creations can be counted.
 */
class AvroDecodeRowsTest {

  private static final Schema ORDER =
      SchemaBuilder.record("order")
          .fields()
          .requiredLong("id")
          .optionalString("name")
          .requiredDouble("amount")
          .endRecord();

  /** The same record with amount as a string, as after a schema change in the stream. */
  private static final Schema ORDER_AMOUNT_AS_STRING =
      SchemaBuilder.record("order")
          .fields()
          .requiredLong("id")
          .optionalString("name")
          .requiredString("amount")
          .endRecord();

  private TransformMockHelper<AvroDecodeMeta, AvroDecodeData> tmh;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    tmh = new TransformMockHelper<>("Avro decode", AvroDecodeMeta.class, AvroDecodeData.class);
    when(tmh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(tmh.iLogChannel);
    when(tmh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    tmh.cleanUp();
  }

  @Test
  void decodesEveryRecordAsBefore() throws Exception {
    List<Object[]> output = decode(rows());

    assertEquals(
        List.of(
            List.of(1L, "a", "1.50"),
            Arrays.asList(2L, null, "2.25"),
            List.of(3L, "c", "12.5"),
            List.of(4L, "d", "0.50")),
        output.stream().map(row -> Arrays.asList(row[1], row[2], row[3])).toList());
  }

  @Test
  void buildsTheValueMetadataOncePerFieldAndType() throws Exception {
    try (MockedStatic<ValueMetaFactory> factory =
        mockStatic(ValueMetaFactory.class, CALLS_REAL_METHODS)) {
      decode(rows());

      // The "standard" metadata: one per target field and type that arrived (amount came as a
      // number and as a string).
      factory.verify(() -> ValueMetaFactory.createValueMeta(eq("standard"), anyInt()), times(4));
      // The target metadata: once in getFields() and once for the conversion, per target field.
      // (The two-argument createValueMeta() above passes its calls on to this one, hence the name.)
      factory.verify(
          () ->
              ValueMetaFactory.createValueMeta(
                  argThat(name -> !"standard".equals(name)), anyInt(), anyInt(), anyInt()),
          times(6));
    }
  }

  /** Four records; the third has amount as a string, as after a schema change in the stream. */
  private static List<Object[]> rows() {
    return List.of(
        new Object[] {order(ORDER, 1L, "a", 1.5)},
        new Object[] {order(ORDER, 2L, null, 2.25)},
        new Object[] {order(ORDER_AMOUNT_AS_STRING, 3L, "c", new Utf8("12.5"))},
        new Object[] {order(ORDER, 4L, "d", 0.5)});
  }

  private static GenericRecord order(Schema schema, long id, String name, Object amount) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", id);
    record.put("name", name == null ? null : new Utf8(name));
    record.put("amount", amount);
    return record;
  }

  private List<Object[]> decode(List<Object[]> rows) throws Exception {
    AvroDecodeMeta meta = new AvroDecodeMeta();
    meta.setSourceFieldName("avro");
    meta.setTargetFields(
        List.of(
            new TargetField("id", null, "id", "Integer", null, null, null),
            new TargetField("name", null, "name", "String", null, null, null),
            new TargetField("amount", null, "amount", "String", "0.00", null, null)));

    AvroDecode transform =
        new AvroDecode(
            tmh.transformMeta, meta, new AvroDecodeData(), 0, tmh.pipelineMeta, tmh.pipeline);
    transform.init();
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaAvroRecord("avro"));
    IRowSet rowSet = tmh.getMockInputRowSet(rows);
    when(rowSet.getRowMeta()).thenReturn(inputRowMeta);
    transform.addRowSetToInputRowSets(rowSet);

    List<Object[]> output = new ArrayList<>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            output.add(row);
          }
        });
    while (transform.processRow()) {
      // one call per record
    }
    assertEquals(0, transform.getErrors());
    return output;
  }
}
