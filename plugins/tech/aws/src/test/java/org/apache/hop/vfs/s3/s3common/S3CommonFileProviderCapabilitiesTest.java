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

package org.apache.hop.vfs.s3.s3common;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.vfs2.Capability;
import org.junit.jupiter.api.Test;

class S3CommonFileProviderCapabilitiesTest {

  /**
   * commons-vfs2 rejects {@code getOutputStream(true)} with "does not support append mode" unless
   * the file system advertises {@link Capability#APPEND_CONTENT}. Append is emulated with a
   * multipart upload (see {@link S3AppendOutputStream}), so the capability must be present or every
   * append is blocked before reaching the provider.
   */
  @Test
  void appendContentIsAdvertised() {
    assertTrue(
        S3CommonFileProvider.capabilities.contains(Capability.APPEND_CONTENT),
        "S3 VFS must advertise APPEND_CONTENT so commons-vfs2 allows append mode");
  }
}
