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

package org.apache.hop.pipeline.transforms.execprocess;

import org.apache.hop.core.SingleRowRowSet;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ExecProcessTest {
  private TransformMockHelper<ExecProcessMeta, ExecProcessData> tmh;
  private IRowMeta rowRowSet;

  @Before
  public void setUp() {
    tmh = new TransformMockHelper<>("execProcess", ExecProcessMeta.class, ExecProcessData.class);
    when(tmh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(tmh.iLogChannel);
    when(tmh.pipeline.isRunning()).thenReturn(true);
    when(tmh.iTransformMeta.getProcessField()).thenReturn("p");
    when(tmh.iTransformMeta.getArgumentFieldNames()).thenReturn(new String[] {"arg"});
    when(tmh.iTransformMeta.isArgumentsInFields()).thenReturn(true);
    when(tmh.iTransformMeta.getResultFieldName()).thenReturn("r1");
    tmh.iTransformData.runtime = Runtime.getRuntime();

    rowRowSet = new RowMeta();
    rowRowSet.addValueMeta(new ValueMetaString("p"));
    rowRowSet.addValueMeta(new ValueMetaString("arg"));
  }

  @Test
  public void testNormalProcess() throws Exception {
    ExecProcess echoProcess = createExecProcess("echo", "'a echo message'");
    assertTrue(echoProcess.init());
    assertTrue(echoProcess.processRow());
    assertFalse(echoProcess.processRow());
    assertTrue(echoProcess.outputIsDone());
  }

  @Test
  public void testHandlingProcess() throws Exception {
    ExecProcess echoProcess = createExecProcess("sleep", "30");
    assertTrue(echoProcess.init());
    CountDownLatch waitingLatch = new CountDownLatch(1);
    Executors.newSingleThreadScheduledExecutor()
        .schedule(
            () -> {
              try {
                echoProcess.stopRunning();
              } catch (Exception ignore) {
              }
              waitingLatch.countDown();
            },
            100,
            TimeUnit.MILLISECONDS);
    echoProcess.processRow();
    waitingLatch.await();
    verify(tmh.iLogChannel).logMinimal(anyString());
  }

  private ExecProcess createExecProcess(String shellCmd, String arg) {
    ExecProcess execEcho =
        new ExecProcess(
            tmh.transformMeta,
            tmh.iTransformMeta,
            tmh.iTransformData,
            0,
            tmh.pipelineMeta,
            tmh.pipeline);
    execEcho.setInputRowMeta(rowRowSet);

    SingleRowRowSet rs = new SingleRowRowSet();
    rs.putRow(rowRowSet, new Object[] {shellCmd, arg});
    rs.setDone();
    execEcho.setInputRowMeta(rowRowSet);
    execEcho.setInputRowSets(new ArrayList<>(Collections.singletonList(rs)));
    return execEcho;
  }
}
