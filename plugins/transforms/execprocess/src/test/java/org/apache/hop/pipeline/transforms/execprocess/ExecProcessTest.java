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

import org.apache.hop.core.Const;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

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
    String[] argFields = Const.isWindows() ? "arg1 arg2 arg3".split(" ") : new String[] {"arg"};
    when(tmh.iTransformMeta.getArgumentFieldNames()).thenReturn(argFields);
    when(tmh.iTransformMeta.isArgumentsInFields()).thenReturn(true);
    when(tmh.iTransformMeta.getResultFieldName()).thenReturn("r1");
    tmh.iTransformData.runtime = Runtime.getRuntime();

    rowRowSet = new RowMeta();
    rowRowSet.addValueMeta(new ValueMetaString("p"));
    for (String field : argFields) {
      rowRowSet.addValueMeta(new ValueMetaString(field));
    }
  }

  @Test
  public void testNormalProcess() throws Exception {
    ExecProcess echoProcess =
        Const.isWindows()
            ? createExecProcess("cmd", "/c", "echo", "a echo message")
            : createExecProcess("echo", "a echo message");
    assertTrue(echoProcess.init());
    assertTrue(echoProcess.processRow());
    assertFalse(echoProcess.processRow());
    assertTrue(echoProcess.outputIsDone());
  }

  @Test
  public void testHandlingProcess() throws Exception {
    ExecProcess echoProcess =
        Const.isWindows()
            ? createExecProcess("cmd", "/c", "pause", "")
            : createExecProcess("sleep", "30");
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
    assertTrue(waitingLatch.await(10, TimeUnit.SECONDS));
  }

  private ExecProcess createExecProcess(String shellCmd, String... args) {
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
    Object[] data = new Object[args.length + 1];
    data[0] = shellCmd;
    System.arraycopy(args, 0, data, 1, args.length);
    rs.putRow(rowRowSet, data);
    rs.setDone();
    execEcho.setInputRowMeta(rowRowSet);
    execEcho.setInputRowSets(new ArrayList<>(Collections.singletonList(rs)));
    return execEcho;
  }
}
