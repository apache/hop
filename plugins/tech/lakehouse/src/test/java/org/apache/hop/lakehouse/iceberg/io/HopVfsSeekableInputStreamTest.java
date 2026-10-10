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

package org.apache.hop.lakehouse.iceberg.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Test;

class HopVfsSeekableInputStreamTest {

  private static final byte[] DATA = "0123456789".getBytes(StandardCharsets.US_ASCII);

  /**
   * S3 and MinIO report random access, but their file objects don't implement it and the VFS
   * default throws. The stream falls back to reopening and skipping.
   */
  @Test
  void fallsBackWhenRandomAccessIsReportedButNotImplemented() throws Exception {
    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.hasCapability(Capability.RANDOM_ACCESS_READ)).thenReturn(true);
    FileContent content = mock(FileContent.class);
    when(content.getRandomAccessContent(any()))
        .thenThrow(new FileSystemException("vfs.provider/random-access-not-supported.error"));
    when(content.getInputStream()).thenAnswer(invocation -> new ByteArrayInputStream(DATA));
    FileObject file = mock(FileObject.class);
    when(file.getFileSystem()).thenReturn(fileSystem);
    when(file.getContent()).thenReturn(content);

    try (HopVfsSeekableInputStream in = new HopVfsSeekableInputStream(file)) {
      in.seek(7);
      assertEquals('7', in.read());
      in.seek(2);
      assertEquals('2', in.read());
      assertEquals(3, in.getPos());
    }
    // Random access is tried once, not again on every reopen.
    verify(content, times(1)).getRandomAccessContent(any());
  }
}
