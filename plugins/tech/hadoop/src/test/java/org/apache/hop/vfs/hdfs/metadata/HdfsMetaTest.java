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
package org.apache.hop.vfs.hdfs.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.vfs.hdfs.HdfsTransport;
import org.junit.jupiter.api.Test;

class HdfsMetaTest {

  @Test
  void defaultsPreferHttpFs() {
    HdfsMeta meta = new HdfsMeta();
    assertEquals(HdfsTransport.HttpFS, meta.getTransport());
    assertEquals("hop", meta.getSimpleUser());
    assertTrue(meta.isHostnameVerification());
    assertFalse(meta.isKerberosEnabled());
    assertEquals("360", meta.getRenewalIntervalMinutes());
  }

  @Test
  void transportDefaults() {
    assertEquals(14000, HdfsTransport.HttpFS.defaultPort());
    assertEquals(8443, HdfsTransport.Knox.defaultPort());
    assertEquals(9870, HdfsTransport.WebHDFS.defaultPort());
    assertTrue(HdfsTransport.HttpFS.dataOnCreateRequest());
    assertFalse(HdfsTransport.WebHDFS.dataOnCreateRequest());
    assertEquals(HdfsTransport.HttpFS, HdfsTransport.fromCode(null));
    assertEquals(HdfsTransport.Knox, HdfsTransport.fromCode("knox"));
  }
}
