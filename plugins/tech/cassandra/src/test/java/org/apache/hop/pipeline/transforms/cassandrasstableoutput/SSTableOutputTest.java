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
 *
 */
package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class SSTableOutputTest {
  private static TransformMockHelper<SSTableOutputMeta, SSTableOutputData> helper;
  private static final SecurityManager sm = System.getSecurityManager();

  @BeforeClass
  public static void setUp() throws HopException {
    HopEnvironment.init();
    helper =
        new TransformMockHelper<>(
            "SSTableOutputIT", SSTableOutputMeta.class, SSTableOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.logChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    when(helper.pipeline.getVariableNames()).thenReturn(new String[0]);
  }

  @AfterClass
  public static void classTearDown() {
    // Cleanup class setup
    helper.cleanUp();
  }

  @After
  public void tearDown() throws Exception {
    // Restore original security manager if needed
    if (System.getSecurityManager() != sm) {
      System.setSecurityManager(sm);
    }
  }

  @Test(expected = SecurityException.class)
  public void testDisableSystemExit() throws Exception {
    SSTableOutput ssTableOutput =
        new SSTableOutput(
            helper.transformMeta,
            helper.iTransformMeta,
            helper.data,
            0,
            helper.pipelineMeta,
            helper.pipeline);
    ssTableOutput.disableSystemExit(sm, helper.logChannel);
    System.exit(1);
  }
}
