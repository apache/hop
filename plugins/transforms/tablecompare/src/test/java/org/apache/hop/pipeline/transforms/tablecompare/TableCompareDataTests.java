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

package org.apache.hop.pipeline.transforms.tablecompare;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.transform.ITransformData;
import org.junit.jupiter.api.Test;

/** Unit test for {@link TableCompareData} */
class TableCompareDataTests {

  @Test
  void implementsTransformData() {
    TableCompareData data = new TableCompareData();
    assertInstanceOf(ITransformData.class, data);
  }

  @Test
  void defaultConstructor_initializesBaseStatusAndNullReferences() {
    TableCompareData data = new TableCompareData();

    assertEquals(ComponentExecutionStatus.STATUS_EMPTY, data.getStatus());
    assertTrue(data.isEmpty());

    assertNull(data.outputRowMeta);
    assertNull(data.convertRowMeta);
    assertNull(data.referenceDb);
    assertNull(data.compareDb);
    assertNull(data.errorRowMeta);

    assertEquals(0, data.refSchemaIndex);
    assertEquals(0, data.refTableIndex);
    assertEquals(0, data.refCteIndex);
    assertEquals(0, data.cmpSchemaIndex);
    assertEquals(0, data.cmpTableIndex);
    assertEquals(0, data.cmpCteIndex);
    assertEquals(0, data.keyFieldsIndex);
    assertEquals(0, data.excludeFieldsIndex);
    assertEquals(0, data.keyDescIndex);
    assertEquals(0, data.valueReferenceIndex);
    assertEquals(0, data.valueCompareIndex);
  }

  @Test
  void rowMetaAndDatabaseFields_canBeAssigned() {
    TableCompareData data = new TableCompareData();

    IRowMeta out = mock(IRowMeta.class);
    IRowMeta convert = mock(IRowMeta.class);
    IRowMeta err = mock(IRowMeta.class);
    Database refDb = mock(Database.class);
    Database cmpDb = mock(Database.class);

    data.outputRowMeta = out;
    data.convertRowMeta = convert;
    data.errorRowMeta = err;
    data.referenceDb = refDb;
    data.compareDb = cmpDb;

    assertEquals(out, data.outputRowMeta);
    assertEquals(convert, data.convertRowMeta);
    assertEquals(err, data.errorRowMeta);
    assertEquals(refDb, data.referenceDb);
    assertEquals(cmpDb, data.compareDb);
  }

  @Test
  void columnIndexFields_canBeAssigned() {
    TableCompareData data = new TableCompareData();

    data.refSchemaIndex = 1;
    data.refTableIndex = 2;
    data.refCteIndex = 3;
    data.cmpSchemaIndex = 4;
    data.cmpTableIndex = 5;
    data.cmpCteIndex = 6;
    data.keyFieldsIndex = 7;
    data.excludeFieldsIndex = 8;
    data.keyDescIndex = 9;
    data.valueReferenceIndex = 10;
    data.valueCompareIndex = 11;

    assertEquals(1, data.refSchemaIndex);
    assertEquals(2, data.refTableIndex);
    assertEquals(3, data.refCteIndex);
    assertEquals(4, data.cmpSchemaIndex);
    assertEquals(5, data.cmpTableIndex);
    assertEquals(6, data.cmpCteIndex);
    assertEquals(7, data.keyFieldsIndex);
    assertEquals(8, data.excludeFieldsIndex);
    assertEquals(9, data.keyDescIndex);
    assertEquals(10, data.valueReferenceIndex);
    assertEquals(11, data.valueCompareIndex);
  }

  @Test
  void statusFollowsBaseTransformData() {
    TableCompareData data = new TableCompareData();
    data.setStatus(ComponentExecutionStatus.STATUS_RUNNING);
    assertEquals(ComponentExecutionStatus.STATUS_RUNNING, data.getStatus());
    assertTrue(data.isRunning());
  }
}
