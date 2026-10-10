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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** The "Maximum connections" setting of an S3 connection. */
class S3CommonFileSystemMaxConnectionsTest {

  @Test
  void blankKeepsTheSdkDefault() {
    assertNull(S3CommonFileSystem.maxConnections(null));
    assertNull(S3CommonFileSystem.maxConnections(" "));
  }

  @Test
  void parsesANumber() {
    assertEquals(100, S3CommonFileSystem.maxConnections("100"));
    assertEquals(8, S3CommonFileSystem.maxConnections(" 8 "));
  }

  @Test
  void ignoresAnInvalidValue() {
    assertNull(S3CommonFileSystem.maxConnections("lots"));
    assertNull(S3CommonFileSystem.maxConnections("0"));
    assertNull(S3CommonFileSystem.maxConnections("20000"));
  }
}
