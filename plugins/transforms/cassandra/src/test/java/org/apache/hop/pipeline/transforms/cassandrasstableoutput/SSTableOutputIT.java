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
package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.pipeline.transforms.steps.mock.StepMockHelper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SSTableOutputIT {
  private static StepMockHelper<SSTableOutputMeta, SSTableOutputData> helper;
  private static AtomicInteger i;

  @BeforeClass
  public static void setUp() throws HopException {
    HopEnvironment.init();
    helper =
        new StepMockHelper<SSTableOutputMeta, SSTableOutputData>(
            "SSTableOutputIT", SSTableOutputMeta.class, SSTableOutputData.class);
    when(helper.logChannelInterfaceFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.logChannelInterface);
    when(helper.trans.isRunning()).thenReturn(true);
  }

  @Test
  @Ignore
  public void testCQLS2SSTableWriter() throws Exception {
    SSTableOutput ssTableOutput =
        new SSTableOutput(
            helper.stepMeta, null, helper.stepDataInterface, 0, helper.transMeta, helper.trans);
    IValueMeta one = new ValueMetaBase("key", ValueMetaBase.TYPE_INTEGER);
    IValueMeta two = new ValueMetaBase("two", ValueMetaBase.TYPE_STRING);
    List<IValueMeta> valueMetaList = new ArrayList<IValueMeta>();
    valueMetaList.add(one);
    valueMetaList.add(two);
    String[] fieldNames = new String[] {"key", "two"};
    IRowMeta inputRowMeta = mock(IRowMeta.class);
    when(inputRowMeta.clone()).thenReturn(inputRowMeta);
    when(inputRowMeta.size()).thenReturn(2);
    when(inputRowMeta.getFieldNames()).thenReturn(fieldNames);
    when(inputRowMeta.getValueMetaList()).thenReturn(valueMetaList);
    IRowSet rowset = helper.getMockInputRowSet(new Object[] {1, "some"});
    when(rowset.getRowMeta()).thenReturn(inputRowMeta);
    ssTableOutput.addRowSetToInputRowSets(rowset);
    ssTableOutput.init();
    ssTableOutput.processRow();
    Assert.assertEquals("Transform init error.", 0, ssTableOutput.getErrors());
    assertEquals(
        "org.apache.hop.pipeline.transforms.cassandrasstableoutput.writer.CQL2SSTableWriter",
        ssTableOutput.writer.getClass().getName());
    ssTableOutput.dispose();
    Assert.assertEquals("Transform dispose error", 0, ssTableOutput.getErrors());
  }

  @Test
  @Ignore
  public void testCQLS3SSTableWriter() throws Exception {
    SSTableOutput ssTableOutput =
        new SSTableOutput(
            helper.stepMeta, null, helper.stepDataInterface, 0, helper.transMeta, helper.trans);
    i = new AtomicInteger(0);
    IValueMeta one = new ValueMetaBase("key", ValueMetaBase.TYPE_INTEGER);
    IValueMeta two = new ValueMetaBase("two", ValueMetaBase.TYPE_STRING);
    List<IValueMeta> valueMetaList = new ArrayList<IValueMeta>();
    valueMetaList.add(one);
    valueMetaList.add(two);
    String[] fieldNames = new String[] {"key", "two"};
    IRowMeta inputRowMeta = mock(IRowMeta.class);
    when(inputRowMeta.clone()).thenReturn(inputRowMeta);
    when(inputRowMeta.size()).thenReturn(2);
    when(inputRowMeta.getFieldNames()).thenReturn(fieldNames);
    when(inputRowMeta.getValueMetaList()).thenReturn(valueMetaList);
    when(inputRowMeta.indexOfValue(anyString()))
        .thenAnswer(
            new Answer<Integer>() {
              @Override
              public Integer answer(InvocationOnMock invocation) throws Throwable {
                return i.getAndIncrement();
              }
            });
    IRowSet rowset = helper.getMockInputRowSet(new Object[] {1L, "some"});
    when(rowset.getRowMeta()).thenReturn(inputRowMeta);
    ssTableOutput.addRowSetToInputRowSets(rowset);
    SSTableOutputMeta meta = createTransformMeta(true);
    ssTableOutput.init();
    ssTableOutput.processRow();
    Assert.assertEquals("Transform init error.", 0, ssTableOutput.getErrors());
    assertEquals(
        "org.apache.hop.pipeline.transforms.cassandrasstableoutput.writer.CQL3SSTableWriter",
        ssTableOutput.writer.getClass().getName());
    ssTableOutput.dispose();
    Assert.assertEquals("Transform dispose error", 0, ssTableOutput.getErrors());
  }

  private SSTableOutputMeta createTransformMeta(Boolean v3) throws IOException {
    File tempFile = File.createTempFile(getClass().getName(), ".tmp");
    tempFile.deleteOnExit();

    final SSTableOutputMeta meta = new SSTableOutputMeta();
    meta.setBufferSize("1000");
    meta.setDirectory(tempFile.getParentFile().toURI().toString());
    meta.setCassandraKeyspace("key");
    meta.setYamlPath(getClass().getResource("cassandra.yaml").getFile());
    meta.setTableName("cfq");
    if (v3) {
      meta.setKeyField("key,two");
    } else {
      meta.setKeyField("key");
    }
    meta.setUseCQL3(v3);

    return meta;
  }
}
