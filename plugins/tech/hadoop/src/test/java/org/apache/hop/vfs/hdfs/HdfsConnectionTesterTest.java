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
package org.apache.hop.vfs.hdfs;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.hdfs.client.WebHdfsTestServer;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;
import org.ietf.jgss.GSSException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HdfsConnectionTesterTest {
  private WebHdfsTestServer server;

  @BeforeEach
  void setUp() throws Exception {
    server = new WebHdfsTestServer();
    server.start();
  }

  @AfterEach
  void tearDown() {
    server.stop();
  }

  @Test
  void clusterProbeHitsWebHdfs() throws Exception {
    HdfsMeta meta = new HdfsMeta();
    meta.setTransport(HdfsTransport.HttpFS);
    meta.setEndpointHostname("127.0.0.1");
    meta.setEndpointPort(Integer.toString(server.getPort()));
    meta.setHttps(false);
    String report = HdfsConnectionTester.testCluster(new Variables(), meta);
    assertTrue(report.contains("HttpFS"));
    assertTrue(report.contains("Succeeded:"));
    assertTrue(report.contains("Plain HTTP"));
    assertTrue(report.contains("GETFILESTATUS"));
    assertTrue(report.contains("DIRECTORY") || report.contains("/"));
  }

  @Test
  void clusterFailureKeepsSucceededStepsAndHint() {
    List<String> ok = new ArrayList<>();
    ok.add("Plain HTTP (TLS not required)");
    ok.add("Kerberos login as hop@EXAMPLE.COM, ticket until -");
    IOException error =
        HdfsConnectionTester.failed(
            ok, "SPNEGO", "HTTP@master1.example.com", new GSSException(GSSException.NO_CRED));
    String message = error.getMessage();
    assertTrue(message.contains("Succeeded:"));
    assertTrue(message.contains("Kerberos login as hop@EXAMPLE.COM"));
    assertTrue(message.contains("Failed: SPNEGO HTTP@master1.example.com"));
    assertTrue(message.contains("HTTP@master1.example.com"));
    assertTrue(message.contains("FQDN") || message.contains("service ticket"));
  }

  @Test
  void clusterProbeRequiresHost() {
    HdfsMeta meta = new HdfsMeta();
    assertThrows(
        IllegalArgumentException.class,
        () -> HdfsConnectionTester.testCluster(new Variables(), meta));
  }

  @Test
  void kerberosProbeRequiresEnable() {
    HdfsMeta meta = new HdfsMeta();
    meta.setKerberosEnabled(false);
    assertThrows(
        IllegalArgumentException.class,
        () -> HdfsConnectionTester.testKerberos(new Variables(), meta));
  }
}
