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
package org.apache.hop.pipeline.transforms.concatfields;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ConcatFieldsTest {

  private TransformMockHelper<ConcatFieldsMeta, ConcatFieldsData> tmh;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  public void setUp() {
    tmh = new TransformMockHelper<>("ConcatFields", ConcatFieldsMeta.class, ConcatFieldsData.class);
    when(tmh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(tmh.iLogChannel);
    when(tmh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  public void cleanUp() {
    tmh.cleanUp();
  }

  @Test
  public void testMissingTrimType() {

    // Create input rowset
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaString field1 = new ValueMetaString("f1");
    inputRowMeta.addValueMeta(field1);
    ValueMetaString field2 = new ValueMetaString("f2");
    inputRowMeta.addValueMeta(field2);
    ValueMetaString field3 = new ValueMetaString("f3");
    inputRowMeta.addValueMeta(field3);

    IRowSet inputRowSet = null;
    inputRowSet = tmh.getMockInputRowSet(new Object[][] {{"A ", "B ", " C "}});
    inputRowSet.setRowMeta(inputRowMeta);

    // Create output rowset
    ConcatFieldsMeta meta = new ConcatFieldsMeta();
    meta.setDefault();
    List<ConcatField> fields = new ArrayList<>();
    ConcatField cf1 = new ConcatField();
    cf1.setName("f1");
    cf1.setType("String");
    fields.add(cf1);
    ConcatField cf2 = new ConcatField();
    cf2.setName("f2");
    cf2.setType("String");
    cf2.setTrimType("both");
    fields.add(cf2);
    ConcatField cf3 = new ConcatField();
    cf3.setName("f3");
    cf3.setType("String");
    cf3.setTrimType("both");
    fields.add(cf3);
    meta.setOutputFields(fields);

    ExtraFields extraFields = new ExtraFields();
    extraFields.setTargetFieldName("fOut");

    meta.setExtraFields(extraFields);

    ConcatFieldsData data = new ConcatFieldsData();
    ConcatFields cfTransform =
        new ConcatFields(tmh.transformMeta, meta, data, 0, tmh.pipelineMeta, tmh.pipeline);
    cfTransform.addRowSetToInputRowSets(inputRowSet);
    cfTransform.setInputRowMeta(inputRowMeta);

    cfTransform.init();

    // Verify field with trim type not specified is present
    try {
      // Use a simple collector instead of anonymous inner class
      TestRowCollector collector = new TestRowCollector();
      cfTransform.addRowListener(collector);
      cfTransform.processRow();

      // Verify the result
      assertEquals(1, collector.getRowsWritten().size());
      Object[] resultRow = collector.getRowsWritten().get(0);
      assertEquals("A ;B;C", resultRow[3]);
    } catch (HopException ke) {
      ke.printStackTrace();
      fail();
    }
  }

  /** Simple test row collector to avoid anonymous inner class compilation issues */
  private static class TestRowCollector extends RowAdapter {
    private final List<Object[]> rowsWritten = new ArrayList<>();

    @Override
    public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      rowsWritten.add(row);
    }

    public List<Object[]> getRowsWritten() {
      return rowsWritten;
    }
  }
}
