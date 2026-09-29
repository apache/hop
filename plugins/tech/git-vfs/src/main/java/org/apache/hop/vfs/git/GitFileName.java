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
package org.apache.hop.vfs.git;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

/**
 * The name of a file behind a named git connection.
 *
 * <p>The repository, the revision and the credentials are reached through the {@link GitCheckout}
 * carried along, but they are deliberately kept out of the URI: {@code ops:///workflows/daily.hwf}
 * is what the user typed and what they get back from {@link #getURI()}, whichever repository the
 * connection happens to point at today. That also means a URI taken from a file object can be
 * handed straight back to VFS.
 *
 * <p>Extends {@link AbstractFileName} rather than {@code GenericFileName}: a git repository has no
 * host, no port and no user, and the generic name would insist on carrying all three into every URI
 * it builds.
 */
public class GitFileName extends AbstractFileName {

  private final boolean readOnly;
  private final GitCheckout checkout;

  protected GitFileName(
      String scheme, String path, FileType type, boolean readOnly, GitCheckout checkout) {
    super(scheme, path, type == null ? FileType.IMAGINARY : type);
    this.readOnly = readOnly;
    this.checkout = checkout;
  }

  /** Whether the connection refuses writes. */
  public boolean isReadOnly() {
    return readOnly;
  }

  /** The repository behind this name, used by the file object to reach its folder on disk. */
  public GitCheckout getCheckout() {
    return checkout;
  }

  /** {@code <connection name>://}, without repository, revision or credentials. */
  @Override
  protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
    buffer.append(getScheme()).append("://");
  }

  @Override
  public FileName createName(String absPath, FileType type) {
    return new GitFileName(getScheme(), absPath, type, readOnly, checkout);
  }
}
