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
 *
 */

package org.apache.hop.vfs.gs;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Collection;
import java.util.Set;

public class GoogleStorageFileProvider extends AbstractOriginatingFileProvider {

  public static final Collection<Capability> capabilities =
      Set.of(
              Capability.CREATE,
              Capability.DELETE,
              Capability.GET_TYPE,
              Capability.GET_LAST_MODIFIED,
              Capability.SET_LAST_MODIFIED_FILE,
              Capability.SET_LAST_MODIFIED_FOLDER,
              Capability.LIST_CHILDREN,
              Capability.READ_CONTENT,
              Capability.URI,
              Capability.WRITE_CONTENT);

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return new GoogleStorageFileSystem(rootName, null, fileSystemOptions);
  }
}
