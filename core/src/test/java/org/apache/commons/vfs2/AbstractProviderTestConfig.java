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
package org.apache.commons.vfs2;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;

/** A partial {@link org.apache.commons.vfs2.ProviderTestConfig} implementation. */
public abstract class AbstractProviderTestConfig extends AbstractProviderTestCase
    implements ProviderTestConfig {

  public static final String MIME_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

  public static final String MIME_TYPE_APPLICATION_X_TAR = "application/x-tar";

  public static final String MIME_TYPE_APPLICATION_ZIP = "application/zip";

  public static String getLocalCanonicalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  public static String getLocalHostUriString(final String scheme, final int socketPort)
      throws UnknownHostException {
    return scheme + "://" + getLocalCanonicalHostName() + ":" + socketPort;
  }

  private FilesCache filesCache;

  /**
   * Subclasses can override.
   *
   * @return A new cache.
   */
  protected FilesCache createFilesCache() {
    return new SoftRefFilesCache();
  }

  /** Returns a DefaultFileSystemManager instance (or subclass instance). */
  @Override
  public DefaultFileSystemManager getDefaultFileSystemManager() {
    return new DefaultFileSystemManager();
  }

  @Override
  public final FilesCache getFilesCache() {
    if (filesCache == null) {
      filesCache = createFilesCache();
    }
    return filesCache;
  }

  @Override
  public boolean isFileSystemRootAccessible() {
    return true;
  }

  /** Prepares the file system manager. This implementation does nothing. */
  @Override
  public void prepare(final DefaultFileSystemManager manager) throws Exception {
    // default is do nothing.
  }

  public void tearDown() throws Exception {
    if (filesCache != null) {
      filesCache.close();
      // Give a chance for any threads to end.
      Thread.sleep(20);
    }
  }
}
