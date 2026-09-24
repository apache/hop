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

package org.apache.hop.vfs.sftp.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jcraft.jsch.SftpATTRS;
import java.util.Map;
import org.apache.hop.core.vfs.VfsFileAttributes;
import org.junit.jupiter.api.Test;

class SftpFileObjectAttributesTest {

  @Test
  void cachedAttributesComeFromAttrsWithoutAStat() {
    SftpATTRS attrs = new SftpATTRS();
    attrs.setUIDGID(1000, 1000);
    attrs.setPERMISSIONS(0644);

    Map<String, Object> attributes = SftpFileObject.cachedAttributes(attrs);

    assertEquals("1000", attributes.get(VfsFileAttributes.OWNER));
    assertEquals(attrs.getPermissionsString(), attributes.get(VfsFileAttributes.PERMISSIONS));
  }

  @Test
  void missingAttrsPublishNothing() {
    assertTrue(SftpFileObject.cachedAttributes(null).isEmpty());

    SftpATTRS sizeOnly = new SftpATTRS();
    sizeOnly.setSIZE(12);
    Map<String, Object> attributes = SftpFileObject.cachedAttributes(sizeOnly);
    assertFalse(attributes.containsKey(VfsFileAttributes.OWNER));
    assertFalse(attributes.containsKey(VfsFileAttributes.PERMISSIONS));
  }
}
