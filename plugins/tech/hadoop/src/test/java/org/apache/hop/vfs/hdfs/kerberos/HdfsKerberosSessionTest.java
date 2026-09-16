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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HdfsKerberosSessionTest {

  private static final String KRB5_CONF = "java.security.krb5.conf";
  private static final String KRB5_REALM = "java.security.krb5.realm";
  private String previousKrb5;
  private String previousRealm;

  @AfterEach
  void tearDown() {
    HdfsKerberosRenewer.getInstance().shutdown();
    restore(KRB5_CONF, previousKrb5);
    restore(KRB5_REALM, previousRealm);
  }

  @Test
  void windowsKrb5AndKeytabPathsUseForwardSlashes() {
    previousKrb5 = System.getProperty(KRB5_CONF);
    previousRealm = System.getProperty(KRB5_REALM);
    HdfsMeta meta = new HdfsMeta();
    meta.setName("cdp");
    meta.setPrincipal("hop@EXAMPLE.COM");
    meta.setKeytabPath("C:\\Users\\hop\\hop.keytab");
    meta.setKrb5ConfPath("C:\\Users\\hop\\krb5.conf");
    meta.setRealm("OTHER.COM");
    HdfsKerberosSession session = new HdfsKerberosSession(new Variables(), meta);
    HdfsKerberosSession.applyJvmKerberosConfig(new Variables(), meta);
    assertEquals("C:/Users/hop/krb5.conf", System.getProperty(KRB5_CONF));
    assertNotEquals("OTHER.COM", System.getProperty(KRB5_REALM));
    assertEquals("C:/Users/hop/hop.keytab", session.keytabPath());
  }

  @Test
  void unregisterDropsSessionFromRenewer() {
    HdfsMeta meta = new HdfsMeta();
    meta.setName("cdp");
    meta.setPrincipal("hop@EXAMPLE.COM");
    meta.setKeytabPath("/tmp/hop.keytab");
    HdfsKerberosSession session = new HdfsKerberosSession(new Variables(), meta);
    HdfsKerberosRenewer.getInstance().register(session);
    assertTrue(HdfsKerberosRenewer.getInstance().sessions().contains(session));
    session.close();
    assertFalse(HdfsKerberosRenewer.getInstance().sessions().contains(session));
  }

  private static void restore(String key, String previous) {
    if (previous == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, previous);
    }
  }
}
