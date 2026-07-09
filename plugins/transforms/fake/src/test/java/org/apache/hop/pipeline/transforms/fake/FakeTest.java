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

package org.apache.hop.pipeline.transforms.fake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runtime coverage for the {@link Fake} transform: {@code init()} binds the configured generators
 * and {@code processRow()} appends one fake value per field to every incoming row, coerced to the
 * declared Hop value-meta type.
 */
class FakeTest {

  private TransformMockHelper<FakeMeta, FakeData> mockHelper;

  @BeforeEach
  void setup() {
    mockHelper = new TransformMockHelper<>("Fake", FakeMeta.class, FakeData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void processRowAppendsGeneratedFieldsToTheIncomingRow() throws HopException {
    FakeMeta meta = new FakeMeta();
    meta.setLocale("en");
    meta.getFields().add(new FakeField("first_name", "name", "firstName"));
    meta.getFields()
        .add(
            new FakeField(
                "age",
                "number",
                "numberBetween",
                List.of(new FakeArgument("int", "18"), new FakeArgument("int", "65"))));
    FakeData data = new FakeData();

    Fake transform =
        new Fake(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    assertTrue(transform.init(), "init() should bind the configured generators");

    // A single incoming row with one String column.
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    IRowSet inputRowSet = mockHelper.getMockInputRowSet(new Object[] {"row-1"});
    when(inputRowSet.getRowMeta()).thenReturn(inputRowMeta);
    transform.setInputRowSets(new ArrayList<>(List.of(inputRowSet)));

    List<Object[]> written = new ArrayList<>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            written.add(row);
          }
        });

    assertTrue(transform.processRow(), "the incoming row must be processed");
    assertFalse(transform.processRow(), "a null row must end processing");

    assertEquals(1, written.size(), "exactly one row must be emitted");
    Object[] row = written.get(0);
    // Original column plus the two generated fields, in field order.
    assertEquals("row-1", row[0], "the incoming value must be preserved");

    assertInstanceOf(String.class, row[1]);
    assertFalse(((String) row[1]).isEmpty(), "name.firstName must produce a non-empty String");

    assertInstanceOf(Long.class, row[2], "numberBetween must be coerced to a Hop Integer (Long)");
    long age = (Long) row[2];
    assertTrue(age >= 18 && age < 65, "generated age out of range: " + age);
  }

  @Test
  void invalidFieldsAreSkippedAtRuntime() throws HopException {
    FakeMeta meta = new FakeMeta();
    meta.setLocale("en");
    meta.getFields().add(new FakeField("good", "name", "firstName"));
    // Missing topic -> invalid -> must not be bound or produce a column.
    meta.getFields().add(new FakeField("incomplete", "name", null));
    FakeData data = new FakeData();

    Fake transform =
        new Fake(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    assertTrue(transform.init());
    assertEquals(1, data.boundGenerators.size(), "only the valid field should be bound");

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    IRowSet inputRowSet = mockHelper.getMockInputRowSet(new Object[] {"row-1"});
    when(inputRowSet.getRowMeta()).thenReturn(inputRowMeta);
    transform.setInputRowSets(new ArrayList<>(List.of(inputRowSet)));

    List<Object[]> written = new ArrayList<>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            written.add(row);
          }
        });

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    assertEquals(1, written.size());
    // Only the original column and the single valid generated field.
    assertInstanceOf(String.class, written.get(0)[1]);
  }
}
