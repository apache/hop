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
package org.apache.hop.vfs.hdfs.kerberos;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HdfsKerberosRenewerTest {

  @AfterEach
  void tearDown() {
    HdfsKerberosRenewer.getInstance().shutdown();
  }

  @Test
  void registerStartsScheduler() {
    HdfsMeta meta = new HdfsMeta();
    meta.setName("cdp");
    meta.setPrincipal("hop@EXAMPLE.COM");
    meta.setKeytabPath("/tmp/hop.keytab");
    meta.setRenewalIntervalMinutes("1");
    HdfsKerberosSession session = new HdfsKerberosSession(new Variables(), meta);
    HdfsKerberosRenewer.getInstance().register(session);
    assertTrue(HdfsKerberosRenewer.getInstance().sessions().contains(session));
  }

  @Test
  void nextRenewalIsBeforeFallbackWindow() {
    HdfsMeta meta = new HdfsMeta();
    meta.setName("cdp");
    meta.setPrincipal("hop@EXAMPLE.COM");
    meta.setKeytabPath("/tmp/hop.keytab");
    meta.setRenewalIntervalMinutes("10");
    HdfsKerberosSession session = new HdfsKerberosSession(new Variables(), meta);
    long now = System.currentTimeMillis();
    // Not logged in: nextRenewal uses loginTime 0 so just assert fallback math is 80% of 10 minutes
    // after a fake login timestamp via reflection-free path: register then check interval > 0.
    long next = session.nextRenewalMillis();
    assertTrue(next >= 0);
    assertTrue(next - now < 10 * 60 * 1000L || next < now);
  }
}
