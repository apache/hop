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

package org.apache.hop.pipeline.transforms.insertupdate;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/** @see InsertUpdate */
public class InsertUpdateTestLazyConversion {
  TransformMockHelper<InsertUpdateMeta, InsertUpdateData> smh;

  @Before
  public void setUp() {
    smh = new TransformMockHelper<>("insertUpdate", InsertUpdateMeta.class, InsertUpdateData.class);
    when(smh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);
    when(smh.pipeline.isRunning()).thenReturn(true);
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  @Test
  public void testDateLazyConversion() throws HopException {

    Database db = mock(Database.class);

    RowMeta returnRowMeta = new RowMeta();
    doReturn(new Object[] {new Timestamp(System.currentTimeMillis())})
        .when(db)
        .getLookup(any(PreparedStatement.class));
    returnRowMeta.addValueMeta(new ValueMetaDate("TimeStamp"));
    doReturn(returnRowMeta).when(db).getReturnRowMeta();

    ValueMetaString storageMetadata = new ValueMetaString("Date");
    storageMetadata.setConversionMask("yyyy-MM-dd");

    ValueMetaDate valueMeta = new ValueMetaDate("Date");
    valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    valueMeta.setStorageMetadata(storageMetadata);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(valueMeta);

    InsertUpdateMeta transformMeta = smh.iTransformMeta;
    InsertUpdateLookupField mockedIulf = mock(InsertUpdateLookupField.class);
    List<InsertUpdateValue> items = mock(ArrayList.class);
    when(transformMeta.getInsertUpdateLookupField()).thenReturn(mockedIulf);
    when(transformMeta.getInsertUpdateLookupField().getValueFields()).thenReturn(items);
    when(items.get(0)).thenReturn(mock(InsertUpdateValue.class));
    when(items.get(0).isUpdate()).thenReturn(true);

    InsertUpdateData transformData = smh.iTransformData;
    transformData.lookupParameterRowMeta = inputRowMeta;
    transformData.db = db;
    transformData.keynrs = transformData.valuenrs = new int[] {0};
    transformData.keynrs2 = new int[] {-1};
    transformData.updateParameterRowMeta = when(mock(RowMeta.class).size()).thenReturn(2).getMock();

    InsertUpdate transform =
        new InsertUpdate(
            smh.transformMeta,
            smh.iTransformMeta,
            smh.iTransformData,
            0,
            smh.pipelineMeta,
            smh.pipeline);
    transform.setInputRowMeta(inputRowMeta);
    transform.addRowSetToInputRowSets(
        smh.getMockInputRowSet(new Object[] {"2013-12-20".getBytes()}));
    transform.init();
    transform.first = false;
    transform.processRow();
  }
}
