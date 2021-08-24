/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.dropbox;

import com.dropbox.core.v2.DbxClientV2;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import java.util.Collection;

public class DropboxFileSystem extends AbstractFileSystem {

  private final DbxClientV2 client;

  protected DropboxFileSystem(
      final FileName rootName,
      final DbxClientV2 client,
      final FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
    this.client = client;
  }

  @Override
  protected void addCapabilities(final Collection<Capability> caps) {
    caps.addAll(DropboxFileProvider.capabilities);
  }

  @Override
  protected FileObject createFile(final AbstractFileName name) throws FileSystemException {
    return new DropboxFileObject(name, this);
  }

  /* package */ DbxClientV2 getClient() {
    return client;
  }
}
