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
 */
package org.apache.hop.pipeline.transforms.sftpput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.sftpput.SftpPutMeta.AfterSftpPut;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SftpPutMetaTest {

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    SftpPutMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/sftpput-transform.xml", SftpPutMeta.class);

    assertEquals("prod-sftp", meta.getConnection());
    assertEquals("filename", meta.getSourceFileFieldName());
    assertEquals("folder", meta.getRemoteDirectoryFieldName());
    assertEquals("target", meta.getRemoteFilenameFieldName());
    assertTrue(meta.isCreateRemoteFolder());
    assertTrue(meta.isAddFilenameToResult());
    assertEquals(AfterSftpPut.MOVE, meta.getAfterSftpPut());
    assertEquals("archive", meta.getDestinationFolderFieldName());
    assertTrue(meta.isCreateDestinationFolder());
  }

  /** An empty or missing after-upload action is a valid "do nothing", never a null. */
  @Test
  void testAfterSftpPutDefaultsToNothing() {
    SftpPutMeta meta = new SftpPutMeta();
    assertEquals(AfterSftpPut.NOTHING, meta.getAfterSftpPut());

    meta.setAfterSftpPut(null);
    assertEquals(AfterSftpPut.NOTHING, meta.getAfterSftpPut());
  }
}
