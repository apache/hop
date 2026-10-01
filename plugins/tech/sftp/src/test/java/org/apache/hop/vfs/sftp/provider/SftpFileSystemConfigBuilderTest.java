/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.vfs.sftp.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.apache.commons.vfs2.FileSystemOptions;
import org.junit.jupiter.api.Test;

/** Tests {@link SftpFileSystemConfigBuilder}. */
public class SftpFileSystemConfigBuilderTest {

  private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

  @Test
  public void testConnectTimeout() {
    final FileSystemOptions options = new FileSystemOptions();
    final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
    builder.setConnectTimeout(options, ONE_MINUTE);
    assertEquals(ONE_MINUTE, builder.getConnectTimeout(options));
    assertEquals(ONE_MINUTE.toMillis(), (long) builder.getConnectTimeoutMillis(options));
    //
    builder.setConnectTimeoutMillis(options, (int) ONE_MINUTE.toMillis());
    assertEquals(ONE_MINUTE, builder.getConnectTimeout(options));
    assertEquals(ONE_MINUTE.toMillis(), (long) builder.getConnectTimeoutMillis(options));
  }

  @Test
  public void testSessionTimeout() {
    final FileSystemOptions options = new FileSystemOptions();
    final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
    builder.setSessionTimeout(options, ONE_MINUTE);
    assertEquals(ONE_MINUTE, builder.getSessionTimeout(options));
    assertEquals(ONE_MINUTE.toMillis(), (long) builder.getSessionTimeoutMillis(options));
    //
    builder.setSessionTimeoutMillis(options, (int) ONE_MINUTE.toMillis());
    assertEquals(ONE_MINUTE, builder.getSessionTimeout(options));
    assertEquals(ONE_MINUTE.toMillis(), (long) builder.getSessionTimeoutMillis(options));
  }
}
